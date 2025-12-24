use anyhow::Context;

use crate::schema::event::{MarketEvent, Stream};
use crate::util::time;

#[derive(Debug, Clone, Copy)]
pub enum TemplateId {
    Trades = 10000,
    BestBidAsk = 10001,
    DepthSnapshot = 10002,
    DepthDiff = 10003,
}

pub fn decode_frame(bytes: &[u8]) -> anyhow::Result<Vec<MarketEvent>> {
    let mut cursor = Cursor::new(bytes);
    let header = cursor.read_message_header().context("read messageHeader")?;

    match header.template_id {
        x if x == TemplateId::BestBidAsk as u16 => {
            let ev = decode_best_bid_ask(&mut cursor).context("decode BestBidAskStreamEvent")?;
            Ok(vec![ev])
        }
        x if x == TemplateId::DepthSnapshot as u16 => {
            let ev =
                decode_depth_snapshot(&mut cursor).context("decode DepthSnapshotStreamEvent")?;
            Ok(vec![ev])
        }
        x if x == TemplateId::Trades as u16 => {
            let evs = decode_trades(&mut cursor).context("decode TradesStreamEvent")?;
            Ok(evs)
        }
        x if x == TemplateId::DepthDiff as u16 => {
            // Not currently used; ignore to avoid building local book state here.
            Ok(vec![])
        }
        other => anyhow::bail!("unknown templateId={other}"),
    }
}

fn decode_best_bid_ask(cursor: &mut Cursor<'_>) -> anyhow::Result<MarketEvent> {
    let event_time_us = cursor.read_i64().context("eventTime")?;
    let book_update_id = cursor.read_i64().context("bookUpdateId")?;
    let price_exp = cursor.read_i8().context("priceExponent")?;
    let qty_exp = cursor.read_i8().context("qtyExponent")?;

    let bid_price = cursor.read_i64().context("bidPrice")?;
    let bid_qty = cursor.read_i64().context("bidQty")?;
    let ask_price = cursor.read_i64().context("askPrice")?;
    let ask_qty = cursor.read_i64().context("askQty")?;

    let symbol = cursor.read_var_string8().context("symbol")?;

    let local_ts = time::now_ms();
    let time_str = time::now_local_time_str();
    let mut ev = MarketEvent::new("binance", Stream::SpotBook, symbol.to_ascii_uppercase());
    ev.local_ts = local_ts;
    ev.time_str = time_str;
    ev.event_time = Some(event_time_us / 1_000);
    ev.update_id = i64_to_u64(book_update_id);
    ev.bid_px = Some(dec64(bid_price, price_exp));
    ev.bid_qty = Some(dec64(bid_qty, qty_exp));
    ev.ask_px = Some(dec64(ask_price, price_exp));
    ev.ask_qty = Some(dec64(ask_qty, qty_exp));
    Ok(ev)
}

fn decode_depth_snapshot(cursor: &mut Cursor<'_>) -> anyhow::Result<MarketEvent> {
    let event_time_us = cursor.read_i64().context("eventTime")?;
    let book_update_id = cursor.read_i64().context("bookUpdateId")?;
    let price_exp = cursor.read_i8().context("priceExponent")?;
    let qty_exp = cursor.read_i8().context("qtyExponent")?;

    let bids = cursor.read_group_size16().context("bids group header")?;
    let mut bid_levels: Vec<(f64, f64)> = Vec::new();
    for i in 0..(bids.num_in_group as usize) {
        let price = cursor.read_i64().context("bid.price")?;
        let qty = cursor.read_i64().context("bid.qty")?;
        if i < 5 {
            bid_levels.push((dec64(price, price_exp), dec64(qty, qty_exp)));
        }
        let consumed = 16usize;
        if bids.block_length as usize > consumed {
            cursor
                .skip((bids.block_length as usize) - consumed)
                .context("skip bid padding")?;
        }
    }

    let asks = cursor.read_group_size16().context("asks group header")?;
    let mut ask_levels: Vec<(f64, f64)> = Vec::new();
    for i in 0..(asks.num_in_group as usize) {
        let price = cursor.read_i64().context("ask.price")?;
        let qty = cursor.read_i64().context("ask.qty")?;
        if i < 5 {
            ask_levels.push((dec64(price, price_exp), dec64(qty, qty_exp)));
        }
        let consumed = 16usize;
        if asks.block_length as usize > consumed {
            cursor
                .skip((asks.block_length as usize) - consumed)
                .context("skip ask padding")?;
        }
    }

    let symbol = cursor.read_var_string8().context("symbol")?;

    let local_ts = time::now_ms();
    let time_str = time::now_local_time_str();
    let mut ev = MarketEvent::new("binance", Stream::SpotL5, symbol.to_ascii_uppercase());
    ev.local_ts = local_ts;
    ev.time_str = time_str;
    ev.event_time = Some(event_time_us / 1_000);
    ev.update_id = i64_to_u64(book_update_id);

    fill_l5_from_levels(&mut ev, &bid_levels, &ask_levels);
    Ok(ev)
}

fn decode_trades(cursor: &mut Cursor<'_>) -> anyhow::Result<Vec<MarketEvent>> {
    let event_time_us = cursor.read_i64().context("eventTime")?;
    let transact_time_us = cursor.read_i64().context("transactTime")?;
    let price_exp = cursor.read_i8().context("priceExponent")?;
    let qty_exp = cursor.read_i8().context("qtyExponent")?;

    let trades_group = cursor.read_group_size().context("trades group header")?;

    // Each trade entry: id(int64), price(int64), qty(int64), isBuyerMaker(u8)
    // Constant field isBestMatch has no on-wire representation.
    let max_trades_per_frame = 10_000u32;
    let take = trades_group.num_in_group.min(max_trades_per_frame);
    let mut trades: Vec<(u64, f64, f64, bool)> = Vec::new();
    for _ in 0..take {
        let id = cursor.read_i64().context("trade.id")?;
        let price = cursor.read_i64().context("trade.price")?;
        let qty = cursor.read_i64().context("trade.qty")?;
        let is_buyer_maker = cursor.read_u8().context("trade.isBuyerMaker")? != 0;
        let consumed = 8 + 8 + 8 + 1;
        if trades_group.block_length as usize > consumed {
            cursor
                .skip((trades_group.block_length as usize) - consumed)
                .context("skip trade padding")?;
        }

        if let Some(id) = i64_to_u64(id) {
            trades.push((
                id,
                dec64(price, price_exp),
                dec64(qty, qty_exp),
                is_buyer_maker,
            ));
        }
    }
    if trades_group.num_in_group > take {
        let remaining = trades_group.num_in_group - take;
        cursor
            .skip((remaining as usize).saturating_mul(trades_group.block_length as usize))
            .context("skip remaining trades")?;
    }

    let symbol = cursor.read_var_string8().context("symbol")?;
    let symbol = symbol.to_ascii_uppercase();

    let local_ts = time::now_ms();
    let time_str = time::now_local_time_str();

    let mut evs = Vec::with_capacity(trades.len());
    for (id, px, qty, is_buyer_maker) in trades {
        let mut ev = MarketEvent::new("binance", Stream::SpotTrade, symbol.clone());
        ev.local_ts = local_ts;
        ev.time_str = time_str.clone();
        ev.event_time = Some(event_time_us / 1_000);
        ev.trade_time = Some(transact_time_us / 1_000);
        ev.update_id = Some(id);

        // Match existing JSON mapping: m == true => bid side fields.
        if is_buyer_maker {
            ev.bid_px = Some(px);
            ev.bid_qty = Some(qty);
        } else {
            ev.ask_px = Some(px);
            ev.ask_qty = Some(qty);
        }

        evs.push(ev);
    }

    Ok(evs)
}

fn fill_l5_from_levels(ev: &mut MarketEvent, bids: &[(f64, f64)], asks: &[(f64, f64)]) {
    let b = |i: usize| bids.get(i).copied();
    let a = |i: usize| asks.get(i).copied();

    if let Some((px, qty)) = b(0) {
        ev.bid_px = Some(px);
        ev.bid_qty = Some(qty);
        ev.bid1_px = Some(px);
        ev.bid1_qty = Some(qty);
    }
    if let Some((px, qty)) = b(1) {
        ev.bid2_px = Some(px);
        ev.bid2_qty = Some(qty);
    }
    if let Some((px, qty)) = b(2) {
        ev.bid3_px = Some(px);
        ev.bid3_qty = Some(qty);
    }
    if let Some((px, qty)) = b(3) {
        ev.bid4_px = Some(px);
        ev.bid4_qty = Some(qty);
    }
    if let Some((px, qty)) = b(4) {
        ev.bid5_px = Some(px);
        ev.bid5_qty = Some(qty);
    }

    if let Some((px, qty)) = a(0) {
        ev.ask_px = Some(px);
        ev.ask_qty = Some(qty);
        ev.ask1_px = Some(px);
        ev.ask1_qty = Some(qty);
    }
    if let Some((px, qty)) = a(1) {
        ev.ask2_px = Some(px);
        ev.ask2_qty = Some(qty);
    }
    if let Some((px, qty)) = a(2) {
        ev.ask3_px = Some(px);
        ev.ask3_qty = Some(qty);
    }
    if let Some((px, qty)) = a(3) {
        ev.ask4_px = Some(px);
        ev.ask4_qty = Some(qty);
    }
    if let Some((px, qty)) = a(4) {
        ev.ask5_px = Some(px);
        ev.ask5_qty = Some(qty);
    }
}

fn dec64(mantissa: i64, exponent: i8) -> f64 {
    let exp = exponent as i32;
    (mantissa as f64) * 10f64.powi(exp)
}

fn i64_to_u64(v: i64) -> Option<u64> {
    u64::try_from(v).ok()
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
struct MessageHeader {
    block_length: u16,
    template_id: u16,
    schema_id: u16,
    version: u16,
}

#[derive(Debug, Clone, Copy)]
struct GroupSize16 {
    block_length: u16,
    num_in_group: u16,
}

#[derive(Debug, Clone, Copy)]
struct GroupSize {
    block_length: u16,
    num_in_group: u32,
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn read_message_header(&mut self) -> anyhow::Result<MessageHeader> {
        Ok(MessageHeader {
            block_length: self.read_u16().context("blockLength")?,
            template_id: self.read_u16().context("templateId")?,
            schema_id: self.read_u16().context("schemaId")?,
            version: self.read_u16().context("version")?,
        })
    }

    fn read_group_size16(&mut self) -> anyhow::Result<GroupSize16> {
        Ok(GroupSize16 {
            block_length: self.read_u16().context("group.blockLength")?,
            num_in_group: self.read_u16().context("group.numInGroup")?,
        })
    }

    fn read_group_size(&mut self) -> anyhow::Result<GroupSize> {
        Ok(GroupSize {
            block_length: self.read_u16().context("group.blockLength")?,
            num_in_group: self.read_u32().context("group.numInGroup")?,
        })
    }

    fn skip(&mut self, n: usize) -> anyhow::Result<()> {
        if self.remaining() < n {
            anyhow::bail!(
                "unexpected EOF while skipping {n} bytes (remaining={})",
                self.remaining()
            );
        }
        self.pos += n;
        Ok(())
    }

    fn read_u8(&mut self) -> anyhow::Result<u8> {
        if self.remaining() < 1 {
            anyhow::bail!("unexpected EOF reading u8");
        }
        let v = self.bytes[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_i8(&mut self) -> anyhow::Result<i8> {
        Ok(self.read_u8()? as i8)
    }

    fn read_u16(&mut self) -> anyhow::Result<u16> {
        if self.remaining() < 2 {
            anyhow::bail!("unexpected EOF reading u16");
        }
        let b = &self.bytes[self.pos..self.pos + 2];
        self.pos += 2;
        Ok(u16::from_le_bytes([b[0], b[1]]))
    }

    fn read_u32(&mut self) -> anyhow::Result<u32> {
        if self.remaining() < 4 {
            anyhow::bail!("unexpected EOF reading u32");
        }
        let b = &self.bytes[self.pos..self.pos + 4];
        self.pos += 4;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn read_i64(&mut self) -> anyhow::Result<i64> {
        if self.remaining() < 8 {
            anyhow::bail!("unexpected EOF reading i64");
        }
        let b = &self.bytes[self.pos..self.pos + 8];
        self.pos += 8;
        Ok(i64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn read_var_string8(&mut self) -> anyhow::Result<String> {
        let len = self.read_u8()? as usize;
        if self.remaining() < len {
            anyhow::bail!(
                "unexpected EOF reading varString8 (len={len}, remaining={})",
                self.remaining()
            );
        }
        let b = &self.bytes[self.pos..self.pos + len];
        self.pos += len;
        let s = std::str::from_utf8(b).context("utf8")?;
        Ok(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn le_u16(v: u16) -> [u8; 2] {
        v.to_le_bytes()
    }
    fn le_u32(v: u32) -> [u8; 4] {
        v.to_le_bytes()
    }
    fn le_i64(v: i64) -> [u8; 8] {
        v.to_le_bytes()
    }

    fn push_var_string8(buf: &mut Vec<u8>, s: &str) {
        assert!(s.len() <= 255);
        buf.push(s.len() as u8);
        buf.extend_from_slice(s.as_bytes());
    }

    fn push_header(buf: &mut Vec<u8>, block_length: u16, template_id: u16) {
        buf.extend_from_slice(&le_u16(block_length));
        buf.extend_from_slice(&le_u16(template_id));
        buf.extend_from_slice(&le_u16(1)); // schemaId
        buf.extend_from_slice(&le_u16(0)); // version
    }

    #[test]
    fn decode_best_bid_ask_ok() {
        let mut buf = Vec::new();
        push_header(&mut buf, 50, TemplateId::BestBidAsk as u16);
        buf.extend_from_slice(&le_i64(1_700_000_000_000_000)); // us
        buf.extend_from_slice(&le_i64(123)); // updateId
        buf.push(254); // -2
        buf.push(253); // -3
        buf.extend_from_slice(&le_i64(12_345)); // 123.45
        buf.extend_from_slice(&le_i64(10_000)); // 10.0
        buf.extend_from_slice(&le_i64(12_346)); // 123.46
        buf.extend_from_slice(&le_i64(20_000)); // 20.0
        push_var_string8(&mut buf, "btcusdt");

        let evs = decode_frame(&buf).unwrap();
        assert_eq!(evs.len(), 1);
        let ev = &evs[0];
        assert_eq!(ev.exchange, "binance");
        assert!(matches!(ev.stream, Stream::SpotBook));
        assert_eq!(ev.symbol, "BTCUSDT");
        assert_eq!(ev.update_id, Some(123));
        assert_eq!(ev.event_time, Some(1_700_000_000_000));
        assert!((ev.bid_px.unwrap() - 123.45).abs() < 1e-9);
        assert!((ev.ask_px.unwrap() - 123.46).abs() < 1e-9);
        assert!((ev.bid_qty.unwrap() - 10.0).abs() < 1e-9);
        assert!((ev.ask_qty.unwrap() - 20.0).abs() < 1e-9);
        assert!(ev.local_ts > 0);
        assert!(!ev.time_str.is_empty());
    }

    #[test]
    fn decode_depth20_to_l5_ok() {
        let mut buf = Vec::new();
        push_header(&mut buf, 18, TemplateId::DepthSnapshot as u16);
        buf.extend_from_slice(&le_i64(1_700_000_000_000_000)); // us
        buf.extend_from_slice(&le_i64(999)); // updateId
        buf.push(254); // -2
        buf.push(253); // -3

        // bids group (groupSize16Encoding)
        buf.extend_from_slice(&le_u16(16)); // blockLength
        buf.extend_from_slice(&le_u16(5)); // numInGroup
        for i in 0..5 {
            buf.extend_from_slice(&le_i64(10_000 + i)); // 100.xx
            buf.extend_from_slice(&le_i64(1_000 + i)); // 1.xx
        }
        // asks group
        buf.extend_from_slice(&le_u16(16));
        buf.extend_from_slice(&le_u16(5));
        for i in 0..5 {
            buf.extend_from_slice(&le_i64(20_000 + i)); // 200.xx
            buf.extend_from_slice(&le_i64(2_000 + i)); // 2.xx
        }
        push_var_string8(&mut buf, "ethusdt");

        let evs = decode_frame(&buf).unwrap();
        assert_eq!(evs.len(), 1);
        let ev = &evs[0];
        assert!(matches!(ev.stream, Stream::SpotL5));
        assert_eq!(ev.symbol, "ETHUSDT");
        assert_eq!(ev.update_id, Some(999));
        assert!((ev.bid1_px.unwrap() - 100.00).abs() < 1e-9);
        assert!((ev.bid5_px.unwrap() - 100.04).abs() < 1e-9);
        assert!((ev.ask1_px.unwrap() - 200.00).abs() < 1e-9);
        assert!((ev.ask5_px.unwrap() - 200.04).abs() < 1e-9);
        assert!(ev.bid_px.is_some() && ev.ask_px.is_some());
    }

    #[test]
    fn decode_trades_explodes_group() {
        let mut buf = Vec::new();
        push_header(&mut buf, 18, TemplateId::Trades as u16);
        buf.extend_from_slice(&le_i64(1_700_000_000_000_000)); // eventTime us
        buf.extend_from_slice(&le_i64(1_700_000_000_100_000)); // transactTime us
        buf.push(254); // -2
        buf.push(253); // -3

        // groupSizeEncoding: blockLength(uint16) + numInGroup(uint32)
        buf.extend_from_slice(&le_u16(25));
        buf.extend_from_slice(&le_u32(2));

        // trade1: id, price, qty, isBuyerMaker
        buf.extend_from_slice(&le_i64(101));
        buf.extend_from_slice(&le_i64(12_345));
        buf.extend_from_slice(&le_i64(10_000));
        buf.push(1);

        // trade2
        buf.extend_from_slice(&le_i64(102));
        buf.extend_from_slice(&le_i64(12_346));
        buf.extend_from_slice(&le_i64(20_000));
        buf.push(0);

        push_var_string8(&mut buf, "btcusdt");

        let evs = decode_frame(&buf).unwrap();
        assert_eq!(evs.len(), 2);
        assert!(matches!(evs[0].stream, Stream::SpotTrade));
        assert_eq!(evs[0].symbol, "BTCUSDT");
        assert_eq!(evs[0].update_id, Some(101));
        assert!(evs[0].bid_px.is_some());
        assert!(evs[1].ask_px.is_some());
    }
}
