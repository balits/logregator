currently: making SstCursor bounded to, well, a pair of [std::ops::Bound]
this way the cursors in MergeIter start an end inside the range
where our keys live, instead of being filled up with unnecessarry records.

