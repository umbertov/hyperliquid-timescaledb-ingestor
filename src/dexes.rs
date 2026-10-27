//! Loads the perpetual markets of HIP-3 builder-deployed perp dexes, such as `xyz`.
//!
//! `hyperliquid_rust_sdk` 0.6.0 sends `meta` without the `dex` field, so it can load
//! only the first perp dex. This module sends the documented info requests directly:
//!
//! - `{"type":"perpDexs"}` lists the perp dexes. The first entry is `null` for the
//!   first perp dex. The other entries carry the dex `name`.
//! - `{"type":"meta","dex":<name>}` lists the markets of one dex. The docs describe
//!   `dex` as "Perp dex name. Defaults to the empty string which represents the first
//!   perp dex." Builder-dex market names carry the dex prefix, such as `xyz:NVDA`.
//!
//! The WebSocket `trades` and `l2Book` subscriptions and the `fundingHistory` request
//! take that prefixed name as `coin`.
//!
//! No `perpDexs`, `meta` or `spotMeta` field links a builder-dex perp to a spot token.
//! Several spot tokens share a stock ticker (for example `NVDA`, `NVDAX` and `EQNVDA`),
//! so a name match is not proof of a pairing. Builder-dex perps are collected without
//! spot counterparts.

use color_eyre::eyre::{eyre, Result, WrapErr};
use serde::Deserialize;
use std::collections::HashSet;

const HYPERLIQUID_INFO_URL: &str = "https://api.hyperliquid.xyz/info";

#[derive(Debug, Deserialize)]
struct PerpDex {
    name: String,
}

#[derive(Debug, Deserialize)]
struct DexMeta {
    universe: Vec<DexAsset>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DexAsset {
    name: String,
    #[serde(default)]
    is_delisted: bool,
}

/// The active markets of one builder dex, and the count of delisted markets skipped.
#[derive(Debug, PartialEq)]
pub struct DexMarkets {
    pub active: Vec<String>,
    pub delisted_count: usize,
}

/// Parses the `--perp-dexes` value. An empty value selects no builder dex. Fails on an
/// empty entry or a duplicate entry.
pub fn parse_perp_dexes(value: &str) -> Result<Vec<String>> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let mut seen = HashSet::new();
    value
        .split(',')
        .map(|dex| {
            if dex.is_empty() || dex.trim() != dex {
                return Err(eyre!(
                    "--perp-dexes has an empty or padded entry: {value:?}"
                ));
            }
            if !seen.insert(dex) {
                return Err(eyre!("--perp-dexes lists {dex:?} twice"));
            }
            Ok(dex.to_string())
        })
        .collect()
}

/// Parses a `perpDexs` response into builder-dex names. Fails unless the first entry
/// is `null` and every other entry is a named dex.
pub fn parse_perp_dex_names(text: &str) -> Result<Vec<String>> {
    let entries: Vec<Option<PerpDex>> =
        serde_json::from_str(text).wrap_err_with(|| format!("parsing perpDexs: {text}"))?;
    let mut entries = entries.into_iter();
    match entries.next() {
        Some(None) => {}
        _ => return Err(eyre!("perpDexs does not start with null: {text}")),
    }
    entries
        .map(|entry| {
            entry
                .map(|dex| dex.name)
                .ok_or_else(|| eyre!("perpDexs has a null entry after the first: {text}"))
        })
        .collect()
}

/// Parses a `meta` response for builder dex `dex`. Fails if a market name lacks the
/// `<dex>:` prefix or if the dex has no active market.
pub fn parse_dex_markets(dex: &str, text: &str) -> Result<DexMarkets> {
    let meta: DexMeta =
        serde_json::from_str(text).wrap_err_with(|| format!("parsing meta for dex {dex}"))?;
    let prefix = format!("{dex}:");
    let mut active = Vec::new();
    let mut delisted_count = 0;
    for asset in meta.universe {
        if !asset.name.starts_with(&prefix) || asset.name.len() == prefix.len() {
            return Err(eyre!(
                "meta for dex {dex} lists market {:?} without the {prefix:?} prefix",
                asset.name
            ));
        }
        if asset.is_delisted {
            delisted_count += 1;
        } else {
            active.push(asset.name);
        }
    }
    if active.is_empty() {
        return Err(eyre!("meta for dex {dex} lists no active market"));
    }
    Ok(DexMarkets {
        active,
        delisted_count,
    })
}

async fn post_info(client: &reqwest::Client, body: serde_json::Value) -> Result<String> {
    let response = client
        .post(HYPERLIQUID_INFO_URL)
        .json(&body)
        .send()
        .await
        .wrap_err_with(|| format!("sending info request {body}"))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .wrap_err_with(|| format!("reading info response for {body}"))?;
    if !status.is_success() {
        return Err(eyre!("info request {body} returned {status}: {text}"));
    }
    Ok(text)
}

/// Loads the active markets of every dex in `dexes`, in order. Fails if Hyperliquid
/// does not list a requested dex.
pub async fn load_dex_markets(dexes: &[String]) -> Result<Vec<(String, DexMarkets)>> {
    if dexes.is_empty() {
        return Ok(Vec::new());
    }
    let client = reqwest::Client::new();
    let known =
        parse_perp_dex_names(&post_info(&client, serde_json::json!({"type": "perpDexs"})).await?)?;
    let mut loaded = Vec::with_capacity(dexes.len());
    for dex in dexes {
        if !known.contains(dex) {
            return Err(eyre!(
                "Hyperliquid lists no perp dex {dex:?}; known dexes: {}",
                known.join(",")
            ));
        }
        let text = post_info(&client, serde_json::json!({"type": "meta", "dex": dex})).await?;
        loaded.push((dex.clone(), parse_dex_markets(dex, &text)?));
    }
    Ok(loaded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_value_selects_no_dex() {
        assert_eq!(parse_perp_dexes("").unwrap(), Vec::<String>::new());
    }

    #[test]
    fn parses_dex_list() {
        assert_eq!(parse_perp_dexes("xyz,flx").unwrap(), vec!["xyz", "flx"]);
    }

    #[test]
    fn rejects_bad_dex_lists() {
        assert!(parse_perp_dexes("xyz,").is_err());
        assert!(parse_perp_dexes("xyz, flx").is_err());
        assert!(parse_perp_dexes("xyz,xyz").is_err());
    }

    #[test]
    fn parses_perp_dex_names() {
        let text = r#"[null,{"name":"xyz","fullName":"XYZ","deployer":"0x1"},{"name":"flx"}]"#;
        assert_eq!(parse_perp_dex_names(text).unwrap(), vec!["xyz", "flx"]);
    }

    #[test]
    fn rejects_perp_dexes_without_leading_null() {
        assert!(parse_perp_dex_names(r#"[{"name":"xyz"}]"#).is_err());
        assert!(parse_perp_dex_names(r#"[null,null]"#).is_err());
    }

    const XYZ_META: &str = r#"{"universe":[
        {"szDecimals":4,"name":"xyz:XYZ100","maxLeverage":30},
        {"szDecimals":3,"name":"xyz:NVDA","maxLeverage":20},
        {"szDecimals":2,"name":"xyz:VIX","maxLeverage":10,"isDelisted":true}
    ],"marginTables":[],"collateralToken":0}"#;

    #[test]
    fn keeps_active_prefixed_markets() {
        assert_eq!(
            parse_dex_markets("xyz", XYZ_META).unwrap(),
            DexMarkets {
                active: vec!["xyz:XYZ100".to_string(), "xyz:NVDA".to_string()],
                delisted_count: 1,
            }
        );
    }

    #[test]
    fn rejects_market_without_dex_prefix() {
        assert!(parse_dex_markets("xyz", r#"{"universe":[{"name":"NVDA"}]}"#).is_err());
        assert!(parse_dex_markets("xyz", r#"{"universe":[{"name":"xyz:"}]}"#).is_err());
        assert!(parse_dex_markets("flx", XYZ_META).is_err());
    }

    #[test]
    fn rejects_dex_without_active_market() {
        let text = r#"{"universe":[{"name":"xyz:VIX","isDelisted":true}]}"#;
        assert!(parse_dex_markets("xyz", text).is_err());
    }
}
