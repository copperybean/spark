package org.apache.spark.sql.codegen;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

// == Subtree 2 / 2 (maxMethodCodeSize:548; maxConstantPoolSize:179(0.27% used); numInnerClasses:0) ==
// *(2) Project [i_brand_id#7, i_class#10, i_item_sk#0, i_item_desc#4, ws_net_paid_inc_ship#52]
// +- *(2) BroadcastHashJoin [i_item_sk#0], [ws_item_sk#24], Inner, BuildRight, false
//    :- *(2) Filter isnotnull(i_item_sk#0)
//    :  +- *(2) ColumnarToRow
//    :     +- FileScan parquet tpcds_sf1_withdecimal_withdate_withnulls.item[i_item_sk#0,i_item_desc#4,i_brand_id#7,i_class#10] Batched: true, DataFilters: [isnotnull(i_item_sk#0)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/zhangzhihong02/tmp/data/tpcds/sf1-parquet/useDecimal=true,..., PartitionFilters: [], PushedFilters: [IsNotNull(i_item_sk)], ReadSchema: struct<i_item_sk:int,i_item_desc:string,i_brand_id:int,i_class:string>
//    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)),false), [id=#374]
//       +- *(1) Project [ws_item_sk#24, ws_net_paid_inc_ship#52]
//          +- *(1) Filter isnotnull(ws_item_sk#24)
//             +- *(1) ColumnarToRow
//                +- FileScan parquet tpcds_sf1_withdecimal_withdate_withnulls.web_sales[ws_item_sk#24,ws_net_paid_inc_ship#52,ws_sold_date_sk#55] Batched: true, DataFilters: [isnotnull(ws_item_sk#24)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[file:/Users/zhangzhihong02/tmp/data/tpcds/sf1-parquet/useDecimal=true,..., PartitionFilters: [isnotnull(ws_sold_date_sk#55), (ws_sold_date_sk#55 = 2452640)], PushedFilters: [IsNotNull(ws_item_sk)], ReadSchema: struct<ws_item_sk:int,ws_net_paid_inc_ship:decimal(7,2)>
// 
// Generated code:
// public Object generate(Object[] references) {
//   return new GeneratedIteratorForCodegenStage2(references);
// }

// codegenStageId=2
final class BroadcaseHashJoinStage2 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private int columnartorow_batchIdx_0;
  private org.apache.spark.sql.execution.joins.LongHashedRelation bhj_relation_0;
  private org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[] columnartorow_mutableStateArray_2 = new org.apache.spark.sql.execution.vectorized.OnHeapColumnVector[4];
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] columnartorow_mutableStateArray_3 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[4];
  private org.apache.spark.sql.vectorized.ColumnarBatch[] columnartorow_mutableStateArray_1 = new org.apache.spark.sql.vectorized.ColumnarBatch[1];
  private scala.collection.Iterator[] columnartorow_mutableStateArray_0 = new scala.collection.Iterator[1];

  public BroadcaseHashJoinStage2(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    columnartorow_mutableStateArray_0[0] = inputs[0];

    columnartorow_mutableStateArray_3[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 64);
    columnartorow_mutableStateArray_3[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(4, 64);

    bhj_relation_0 = ((org.apache.spark.sql.execution.joins.LongHashedRelation) ((org.apache.spark.broadcast.TorrentBroadcast) references[3] /* broadcast */).value()).asReadOnlyCopy();
    incPeakExecutionMemory(bhj_relation_0.estimatedSize());

    columnartorow_mutableStateArray_3[2] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(6, 64);
    columnartorow_mutableStateArray_3[3] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(5, 64);

  }

  private void columnartorow_nextBatch_0() throws java.io.IOException {
    if (columnartorow_mutableStateArray_0[0].hasNext()) {
      columnartorow_mutableStateArray_1[0] = (org.apache.spark.sql.vectorized.ColumnarBatch)columnartorow_mutableStateArray_0[0].next();
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[1] /* numInputBatches */).add(1);
      ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(columnartorow_mutableStateArray_1[0].numRows());
      columnartorow_batchIdx_0 = 0;
      columnartorow_mutableStateArray_2[0] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(0);
      columnartorow_mutableStateArray_2[1] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(1);
      columnartorow_mutableStateArray_2[2] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(2);
      columnartorow_mutableStateArray_2[3] = (org.apache.spark.sql.execution.vectorized.OnHeapColumnVector) columnartorow_mutableStateArray_1[0].column(3);

    }
  }

  protected void processNext() throws java.io.IOException {
    if (columnartorow_mutableStateArray_1[0] == null) {
      columnartorow_nextBatch_0();
    }
    while ( columnartorow_mutableStateArray_1[0] != null) {
      int columnartorow_numRows_0 = columnartorow_mutableStateArray_1[0].numRows();
      int columnartorow_localEnd_0 = columnartorow_numRows_0 - columnartorow_batchIdx_0;
      for (int columnartorow_localIdx_0 = 0; columnartorow_localIdx_0 < columnartorow_localEnd_0; columnartorow_localIdx_0++) {
        int columnartorow_rowIdx_0 = columnartorow_batchIdx_0 + columnartorow_localIdx_0;
        do {
          boolean columnartorow_isNull_0 = columnartorow_mutableStateArray_2[0].isNullAt(columnartorow_rowIdx_0);
          int columnartorow_value_0 = columnartorow_isNull_0 ? -1 : (columnartorow_mutableStateArray_2[0].getInt(columnartorow_rowIdx_0));

          boolean filter_value_2 = !columnartorow_isNull_0;
          if (!filter_value_2) continue;

          ((org.apache.spark.sql.execution.metric.SQLMetric) references[2] /* numOutputRows */).add(1);

          // generate join key for stream side
          boolean bhj_isNull_0 = false;
          long bhj_value_0 = -1L;
          if (!false) {
            bhj_value_0 = (long) columnartorow_value_0;
          }
          // find matches from HashRelation
          scala.collection.Iterator bhj_matches_0 = bhj_isNull_0 ?
          null : (scala.collection.Iterator)bhj_relation_0.get(bhj_value_0);
          if (bhj_matches_0 != null) {
            while (bhj_matches_0.hasNext()) {
              UnsafeRow bhj_buildRow_0 = (UnsafeRow) bhj_matches_0.next();
              {
                ((org.apache.spark.sql.execution.metric.SQLMetric) references[4] /* numOutputRows */).add(1);

                // common sub-expressions

                boolean columnartorow_isNull_2 = columnartorow_mutableStateArray_2[2].isNullAt(columnartorow_rowIdx_0);
                int columnartorow_value_2 = columnartorow_isNull_2 ? -1 : (columnartorow_mutableStateArray_2[2].getInt(columnartorow_rowIdx_0));
                boolean columnartorow_isNull_3 = columnartorow_mutableStateArray_2[3].isNullAt(columnartorow_rowIdx_0);
                UTF8String columnartorow_value_3 = columnartorow_isNull_3 ? null : (columnartorow_mutableStateArray_2[3].getUTF8String(columnartorow_rowIdx_0));
                boolean columnartorow_isNull_1 = columnartorow_mutableStateArray_2[1].isNullAt(columnartorow_rowIdx_0);
                UTF8String columnartorow_value_1 = columnartorow_isNull_1 ? null : (columnartorow_mutableStateArray_2[1].getUTF8String(columnartorow_rowIdx_0));
                boolean bhj_isNull_3 = bhj_buildRow_0.isNullAt(1);
                Decimal bhj_value_3 = bhj_isNull_3 ?
                null : (bhj_buildRow_0.getDecimal(1, 7, 2));
                columnartorow_mutableStateArray_3[3].reset();

                columnartorow_mutableStateArray_3[3].zeroOutNullBytes();

                if (columnartorow_isNull_2) {
                  columnartorow_mutableStateArray_3[3].setNullAt(0);
                } else {
                  columnartorow_mutableStateArray_3[3].write(0, columnartorow_value_2);
                }

                if (columnartorow_isNull_3) {
                  columnartorow_mutableStateArray_3[3].setNullAt(1);
                } else {
                  columnartorow_mutableStateArray_3[3].write(1, columnartorow_value_3);
                }

                if (false) {
                  columnartorow_mutableStateArray_3[3].setNullAt(2);
                } else {
                  columnartorow_mutableStateArray_3[3].write(2, columnartorow_value_0);
                }

                if (columnartorow_isNull_1) {
                  columnartorow_mutableStateArray_3[3].setNullAt(3);
                } else {
                  columnartorow_mutableStateArray_3[3].write(3, columnartorow_value_1);
                }

                if (bhj_isNull_3) {
                  columnartorow_mutableStateArray_3[3].setNullAt(4);
                } else {
                  columnartorow_mutableStateArray_3[3].write(4, bhj_value_3, 7, 2);
                }
                append((columnartorow_mutableStateArray_3[3].getRow()).copy());

              }
            }
          }

        } while(false);
        if (shouldStop()) { columnartorow_batchIdx_0 = columnartorow_rowIdx_0 + 1; return; }
      }
      columnartorow_batchIdx_0 = columnartorow_numRows_0;
      columnartorow_mutableStateArray_1[0] = null;
      columnartorow_nextBatch_0();
    }
  }

}
