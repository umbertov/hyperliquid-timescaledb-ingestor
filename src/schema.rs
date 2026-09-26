diesel::table! {
    symbols (id) {
        id -> Int4,
        name -> Text,
        market_type -> Text,
    }
}

diesel::table! {
    trades (time, id) {
        id -> Int8,
        time -> Timestamptz,
        symbol -> Int4,
        hyperliquid_trade_id -> Int8,
        trade_hash -> Text,
        side -> Text,
        price -> Numeric,
        size -> Numeric,
    }
}

diesel::table! {
    orderbook_messages (time, id) {
        id -> Int8,
        time -> Timestamptz,
        symbol -> Int4,
        bids -> Jsonb,
        asks -> Jsonb,
    }
}

diesel::joinable!(trades -> symbols (symbol));
diesel::joinable!(orderbook_messages -> symbols (symbol));
diesel::allow_tables_to_appear_in_same_query!(symbols, trades, orderbook_messages);
