use oxide_core::pipeline::archive::reorder_writer::{BoundedReorderWriter, OrderedChunkWriter};
use oxide_core::types::Result;
use oxide_core::OxideError;

#[derive(Default)]
struct CollectWriter {
    output: Vec<u8>,
}

impl OrderedChunkWriter for CollectWriter {
    fn write_chunk(&mut self, bytes: &[u8]) -> Result<()> {
        self.output.extend_from_slice(bytes);
        Ok(())
    }
}

#[test]
fn flushes_blocks_in_id_order() {
    let writer = CollectWriter::default();
    let mut reorder = BoundedReorderWriter::with_limit(writer, 4);

    reorder.push(1, vec![2, 2]).expect("push id=1");
    reorder.push(0, vec![1]).expect("push id=0");
    reorder.push(2, vec![3]).expect("push id=2");

    let (writer, stats) = reorder.finish(3).expect("finish");
    assert_eq!(writer.output, vec![1, 2, 2, 3]);
    assert_eq!(stats.wrote_blocks, 3);
    assert_eq!(stats.wrote_bytes, 4);
    assert!(stats.pending_blocks_peak >= 1);
}

#[test]
fn enforces_pending_limit_for_out_of_order_pushes() {
    let writer = CollectWriter::default();
    let mut reorder = BoundedReorderWriter::with_limit(writer, 1);

    reorder.push(1, vec![1]).expect("first out-of-order push");
    let error = reorder.push(2, vec![2]).expect_err("limit should fail");
    assert!(matches!(
        error,
        OxideError::InvalidFormat("reorder buffer capacity exceeded")
    ));
}
