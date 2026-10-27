//! Loads the manual mapping from builder-dex perps to their spot counterparts.
//!
//! The file format and the selection criterion are in `config/xyz-spot-map.toml`.
//! Parsing rejects unknown keys. Checking against the live API fails if a mapped perp
//! or spot pair does not exist, or if a spot pair has another base token than the file
//! states.

use color_eyre::eyre::{eyre, Result, WrapErr};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::path::Path;

const PERP_PREFIX: &str = "xyz:";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MapFile {
    pairs: Vec<SpotPair>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpotPair {
    pub perp: String,
    pub spot: String,
    pub spot_token: String,
    pub rationale: String,
}

/// Reads and parses the mapping file at `path`.
pub fn load(path: &Path) -> Result<Vec<SpotPair>> {
    let text = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("reading xyz spot map {}", path.display()))?;
    parse(&text).wrap_err_with(|| format!("in xyz spot map {}", path.display()))
}

/// Parses the mapping file text. Fails on an unknown key, a malformed name, an empty
/// field, or a perp or spot pair that appears twice.
pub fn parse(text: &str) -> Result<Vec<SpotPair>> {
    let file: MapFile = toml::from_str(text).wrap_err("parsing xyz spot map")?;
    let mut perps = HashSet::new();
    let mut spots = HashSet::new();
    for pair in &file.pairs {
        let perp_ticker = pair.perp.strip_prefix(PERP_PREFIX).unwrap_or_default();
        if perp_ticker.is_empty() {
            return Err(eyre!(
                "perp {:?} must start with {PERP_PREFIX:?} and name a market",
                pair.perp
            ));
        }
        let spot_index = pair.spot.strip_prefix('@').unwrap_or_default();
        if spot_index.is_empty() || !spot_index.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(eyre!(
                "spot {:?} for {} must be @ followed by the pair index",
                pair.spot,
                pair.perp
            ));
        }
        if pair.spot_token.is_empty() || pair.rationale.is_empty() {
            return Err(eyre!(
                "entry for {} has an empty spot_token or rationale",
                pair.perp
            ));
        }
        if !perps.insert(pair.perp.as_str()) {
            return Err(eyre!("perp {} is mapped twice", pair.perp));
        }
        if !spots.insert(pair.spot.as_str()) {
            return Err(eyre!("spot {} is mapped to two perps", pair.spot));
        }
    }
    Ok(file.pairs)
}

/// Fails unless every pair names a perp in `xyz_perps` and a spot pair in
/// `spot_base_tokens` whose base token is the `spot_token` of the pair.
///
/// `spot_base_tokens` maps a spot pair name, such as `@702`, to its base token name.
pub fn check_against_live(
    pairs: &[SpotPair],
    xyz_perps: &HashSet<&str>,
    spot_base_tokens: &HashMap<&str, &str>,
) -> Result<()> {
    for pair in pairs {
        if !xyz_perps.contains(pair.perp.as_str()) {
            return Err(eyre!(
                "xyz spot map lists {}, which Hyperliquid does not list as an active xyz perp",
                pair.perp
            ));
        }
        let base = spot_base_tokens.get(pair.spot.as_str()).ok_or_else(|| {
            eyre!(
                "xyz spot map lists {}, which spotMeta does not list",
                pair.spot
            )
        })?;
        if *base != pair.spot_token {
            return Err(eyre!(
                "xyz spot map pairs {} with {}, but spot pair {} has base token {}",
                pair.perp,
                pair.spot_token,
                pair.spot,
                base
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[[pairs]]
perp = "xyz:NVDA"
spot = "@702"
spot_token = "NVDAX"
rationale = "NVDAX quoted in USDC."
"#;

    #[test]
    fn parses_valid_entry() {
        let pairs = parse(VALID).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].perp, "xyz:NVDA");
        assert_eq!(pairs[0].spot, "@702");
    }

    #[test]
    fn accepts_empty_pair_list() {
        assert!(parse("pairs = []").unwrap().is_empty());
    }

    #[test]
    fn rejects_unknown_key() {
        let text = format!("{VALID}\nextra = true\n");
        assert!(parse(&text).is_err());
        let text = VALID.replace("rationale", "reason");
        assert!(parse(&text).is_err());
    }

    #[test]
    fn rejects_perp_without_xyz_prefix() {
        let text = VALID.replace("xyz:NVDA", "NVDA");
        assert!(parse(&text).is_err());
        let text = VALID.replace("xyz:NVDA", "xyz:");
        assert!(parse(&text).is_err());
    }

    #[test]
    fn rejects_spot_without_pair_index() {
        let text = VALID.replace("@702", "NVDAX/USDC");
        assert!(parse(&text).is_err());
        let text = VALID.replace("@702", "@");
        assert!(parse(&text).is_err());
    }

    #[test]
    fn rejects_duplicate_perp_and_spot() {
        let duplicate_perp = format!("{VALID}{VALID}");
        assert!(parse(&duplicate_perp).is_err());
        let second = VALID.replace("xyz:NVDA", "xyz:TSLA");
        let duplicate_spot = format!("{VALID}{second}");
        assert!(parse(&duplicate_spot).is_err());
    }

    #[test]
    fn check_accepts_matching_live_entry() {
        let pairs = parse(VALID).unwrap();
        let perps = HashSet::from(["xyz:NVDA"]);
        let spots = HashMap::from([("@702", "NVDAX")]);
        assert!(check_against_live(&pairs, &perps, &spots).is_ok());
    }

    #[test]
    fn check_rejects_missing_perp_spot_or_wrong_base() {
        let pairs = parse(VALID).unwrap();
        let spots = HashMap::from([("@702", "NVDAX")]);
        assert!(check_against_live(&pairs, &HashSet::new(), &spots).is_err());
        let perps = HashSet::from(["xyz:NVDA"]);
        assert!(check_against_live(&pairs, &perps, &HashMap::new()).is_err());
        let wrong_base = HashMap::from([("@702", "NVDA")]);
        assert!(check_against_live(&pairs, &perps, &wrong_base).is_err());
    }
}
