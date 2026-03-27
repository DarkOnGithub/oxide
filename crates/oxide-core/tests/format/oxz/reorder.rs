use super::ReorderBuffer;
use crate::OxideError;

#[test]
fn releases_contiguous_items_once_gap_is_filled() {
    let mut reorder = ReorderBuffer::with_limit(4);

    assert_eq!(reorder.push(1, 11).expect("push id=1"), Vec::<i32>::new());
    assert_eq!(reorder.push(3, 33).expect("push id=3"), Vec::<i32>::new());
    assert_eq!(reorder.push(2, 22).expect("push id=2"), Vec::<i32>::new());
    assert_eq!(reorder.push(0, 0).expect("push id=0"), vec![0, 11, 22, 33]);
    assert_eq!(reorder.pending_len(), 0);
}

#[test]
fn rejects_ids_outside_bounded_window() {
    let mut reorder = ReorderBuffer::with_limit(1);

    let error = reorder
        .push(2, 22)
        .expect_err("gap beyond limit should fail");
    assert!(matches!(
        error,
        OxideError::InvalidFormat("reorder buffer capacity exceeded")
    ));
}

#[test]
fn clear_discards_pending_items() {
    let mut reorder = ReorderBuffer::with_limit(4);
    reorder.push(2, 22).expect("push id=2");
    reorder.push(1, 11).expect("push id=1");

    assert_eq!(reorder.clear(), 2);
    assert_eq!(reorder.pending_len(), 0);
    assert_eq!(reorder.push(0, 0).expect("push id=0"), vec![0]);
}
