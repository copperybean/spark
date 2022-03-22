package org.apache.spark.sql.codegen;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

// from: explain codegen select ss_item_sk, ss_addr_sk, ss_list_price, sr_reason_sk, sr_net_loss from store_sales ss join store_returns sr on ss.ss_item_sk = sr.sr_item_sk limit 10
//
// == Subtree 4 / 5 (maxMethodCodeSize:154; maxConstantPoolSize:130(0.20% used); numInnerClasses:0) ==
// *(4) Sort [sr_item_sk#24 ASC NULLS FIRST], false, 0
// +- Exchange hashpartitioning(sr_item_sk#24, 1), ENSURE_REQUIREMENTS, [id=#344]
//    +- *(3) Project [sr_item_sk#24, sr_reason_sk#30, sr_net_loss#41]
//       +- *(3) Filter isnotnull(sr_item_sk#24)
//          +- *(3) ColumnarToRow
//             +- FileScan parquet tpcds_sf1_withdecimal_withdate_withnulls.store_returns[sr_item_sk#24,sr_reason_sk#30,sr_net_loss#41,sr_returned_date_sk#42] Batched: true, DataFilters: [isnotnull(sr_item_sk#24)], Format: Parquet, Location: CatalogFileIndex(1 paths)[file:/Users/zhangzhihong02/tmp/data/tpcds/sf1-parquet/useDecimal=true,u..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:int,sr_reason_sk:int,sr_net_loss:decimal(7,2)>
// 
// Generated code:
// public Object generate(Object[] references) {
//   return new GeneratedIteratorForCodegenStage4(references);
// }

// codegenStageId=4
final class JoinStage4 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private boolean sort_needToSort_0;
  private org.apache.spark.sql.execution.UnsafeExternalRowSorter sort_sorter_0;
  private org.apache.spark.executor.TaskMetrics sort_metrics_0;
  private scala.collection.Iterator<UnsafeRow> sort_sortedIter_0;
  private scala.collection.Iterator inputadapter_input_0;

  public JoinStage4(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    sort_needToSort_0 = true;
    sort_sorter_0 = ((org.apache.spark.sql.execution.SortExec) references[0] /* plan */).createSorter();
    sort_metrics_0 = org.apache.spark.TaskContext.get().taskMetrics();

    inputadapter_input_0 = inputs[0];

  }

  private void sort_addToSorter_0() throws java.io.IOException {
    while ( inputadapter_input_0.hasNext()) {
      InternalRow inputadapter_row_0 = (InternalRow) inputadapter_input_0.next();

      sort_sorter_0.insertRow((UnsafeRow)inputadapter_row_0);
      // shouldStop check is eliminated
    }

  }

  protected void processNext() throws java.io.IOException {
    if (sort_needToSort_0) {
      long sort_spillSizeBefore_0 = sort_metrics_0.memoryBytesSpilled();
      sort_addToSorter_0();
      // sort_sortedIter_0 = sort_sorter_0.sort();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[3] /* sortTime */).add(sort_sorter_0.getSortTimeNanos() / 1000000);
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* peakMemory */).add(sort_sorter_0.getPeakMemoryUsage());
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* spillSize */).add(sort_metrics_0.memoryBytesSpilled() - sort_spillSizeBefore_0);
      sort_metrics_0.incPeakExecutionMemory(sort_sorter_0.getPeakMemoryUsage());
      sort_needToSort_0 = false;
    }

    while ( sort_sortedIter_0.hasNext()) {
      UnsafeRow sort_outputRow_0 = (UnsafeRow)sort_sortedIter_0.next();

      append(sort_outputRow_0);

      if (shouldStop()) return;
    }
  }

}
