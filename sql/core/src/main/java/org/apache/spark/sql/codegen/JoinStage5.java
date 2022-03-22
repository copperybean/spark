package org.apache.spark.sql.codegen;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;

// explain codegen select ss_item_sk, ss_addr_sk, ss_list_price, sr_reason_sk, sr_net_loss from store_sales ss join store_returns sr on ss.ss_item_sk = sr.sr_item_sk limit 10
//
// == Subtree 5 / 5 (maxMethodCodeSize:447; maxConstantPoolSize:166(0.25% used); numInnerClasses:0) ==
// *(5) Project [ss_item_sk#1, ss_addr_sk#5, ss_list_price#11, sr_reason_sk#30, sr_net_loss#41]
// +- *(5) SortMergeJoin [ss_item_sk#1], [sr_item_sk#24], Inner
//    :- *(2) Sort [ss_item_sk#1 ASC NULLS FIRST], false, 0
//    :  +- Exchange hashpartitioning(ss_item_sk#1, 1), ENSURE_REQUIREMENTS, [id=#334]
//    :     +- *(1) Project [ss_item_sk#1, ss_addr_sk#5, ss_list_price#11]
//    :        +- *(1) Filter isnotnull(ss_item_sk#1)
//    :           +- *(1) ColumnarToRow
//    :              +- FileScan parquet tpcds_sf1_withdecimal_withdate_withnulls.store_sales[ss_item_sk#1,ss_addr_sk#5,ss_list_price#11,ss_sold_date_sk#22] Batched: true, DataFilters: [isnotnull(ss_item_sk#1)], Format: Parquet, Location: CatalogFileIndex(1 paths)[file:/Users/zhangzhihong02/tmp/data/tpcds/sf1-parquet/useDecimal=true,u..., PartitionFilters: [], PushedFilters: [IsNotNull(ss_item_sk)], ReadSchema: struct<ss_item_sk:int,ss_addr_sk:int,ss_list_price:decimal(7,2)>
//    +- *(4) Sort [sr_item_sk#24 ASC NULLS FIRST], false, 0
//       +- Exchange hashpartitioning(sr_item_sk#24, 1), ENSURE_REQUIREMENTS, [id=#344]
//          +- *(3) Project [sr_item_sk#24, sr_reason_sk#30, sr_net_loss#41]
//             +- *(3) Filter isnotnull(sr_item_sk#24)
//                +- *(3) ColumnarToRow
//                   +- FileScan parquet tpcds_sf1_withdecimal_withdate_withnulls.store_returns[sr_item_sk#24,sr_reason_sk#30,sr_net_loss#41,sr_returned_date_sk#42] Batched: true, DataFilters: [isnotnull(sr_item_sk#24)], Format: Parquet, Location: CatalogFileIndex(1 paths)[file:/Users/zhangzhihong02/tmp/data/tpcds/sf1-parquet/useDecimal=true,u..., PartitionFilters: [], PushedFilters: [IsNotNull(sr_item_sk)], ReadSchema: struct<sr_item_sk:int,sr_reason_sk:int,sr_net_loss:decimal(7,2)>
// 
// Generated code:
// public Object generate(Object[] references) {
//   return new GeneratedIteratorForCodegenStage5(references);
// }

// codegenStageId=5
final class JoinStage5 extends org.apache.spark.sql.execution.BufferedRowIterator {
  private Object[] references;
  private scala.collection.Iterator[] inputs;
  private scala.collection.Iterator smj_streamedInput_0;
  private scala.collection.Iterator smj_bufferedInput_0;
  private InternalRow smj_streamedRow_0;
  private InternalRow smj_bufferedRow_0;
  private int smj_value_2;
  private org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray smj_matches_0;
  private int smj_value_3;
  private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[] smj_mutableStateArray_0 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter[2];

  public JoinStage5(Object[] references) {
    this.references = references;
  }

  public void init(int index, scala.collection.Iterator[] inputs) {
    partitionIndex = index;
    this.inputs = inputs;
    smj_streamedInput_0 = inputs[0];
    smj_bufferedInput_0 = inputs[1];

    smj_matches_0 = new org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray(2147483632, 2147483647);
    smj_mutableStateArray_0[0] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(6, 0);
    smj_mutableStateArray_0[1] = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(5, 0);

  }

  private boolean smj_findNextJoinRows_0(
    scala.collection.Iterator streamedIter,
    scala.collection.Iterator bufferedIter) {
    smj_streamedRow_0 = null;
    int comp = 0;
    while (smj_streamedRow_0 == null) {
      if (!streamedIter.hasNext()) return false;
      smj_streamedRow_0 = (InternalRow) streamedIter.next();
      boolean smj_isNull_0 = smj_streamedRow_0.isNullAt(0);
      int smj_value_0 = smj_isNull_0 ?
      -1 : (smj_streamedRow_0.getInt(0));
      if (smj_isNull_0) {
        smj_streamedRow_0 = null;
        continue;

      }
      if (!smj_matches_0.isEmpty()) {
        comp = 0;
        if (comp == 0) {
          comp = (smj_value_0 > smj_value_3 ? 1 : smj_value_0 < smj_value_3 ? -1 : 0);
        }

        if (comp == 0) {
          return true;
        }
        smj_matches_0.clear();
      }

      do {
        if (smj_bufferedRow_0 == null) {
          if (!bufferedIter.hasNext()) {
            smj_value_3 = smj_value_0;
            return !smj_matches_0.isEmpty();
          }
          smj_bufferedRow_0 = (InternalRow) bufferedIter.next();
          boolean smj_isNull_1 = smj_bufferedRow_0.isNullAt(0);
          int smj_value_1 = smj_isNull_1 ?
          -1 : (smj_bufferedRow_0.getInt(0));
          if (smj_isNull_1) {
            smj_bufferedRow_0 = null;
            continue;
          }
          smj_value_2 = smj_value_1;
        }

        comp = 0;
        if (comp == 0) {
          comp = (smj_value_0 > smj_value_2 ? 1 : smj_value_0 < smj_value_2 ? -1 : 0);
        }

        if (comp > 0) {
          smj_bufferedRow_0 = null;
        } else if (comp < 0) {
          if (!smj_matches_0.isEmpty()) {
            smj_value_3 = smj_value_0;
            return true;
          } else {
            smj_streamedRow_0 = null;
          }
        } else {
          smj_matches_0.add((UnsafeRow) smj_bufferedRow_0);
          smj_bufferedRow_0 = null;
        }
      } while (smj_streamedRow_0 != null);
    }
    return false; // unreachable
  }

  protected void processNext() throws java.io.IOException {
    while (smj_findNextJoinRows_0(smj_streamedInput_0, smj_bufferedInput_0)) {
      boolean smj_isNull_2 = false;
      int smj_value_4 = -1;

      boolean smj_isNull_3 = false;
      int smj_value_5 = -1;

      boolean smj_isNull_4 = false;
      Decimal smj_value_6 = null;

      smj_isNull_2 = smj_streamedRow_0.isNullAt(0);
      smj_value_4 = smj_isNull_2 ? -1 : (smj_streamedRow_0.getInt(0));
      smj_isNull_3 = smj_streamedRow_0.isNullAt(1);
      smj_value_5 = smj_isNull_3 ? -1 : (smj_streamedRow_0.getInt(1));
      smj_isNull_4 = smj_streamedRow_0.isNullAt(2);
      smj_value_6 = smj_isNull_4 ? null : (smj_streamedRow_0.getDecimal(2, 7, 2));
      scala.collection.Iterator<UnsafeRow> smj_iterator_0 = smj_matches_0.generateIterator();

      while (smj_iterator_0.hasNext()) {
        InternalRow smj_bufferedRow_1 = (InternalRow) smj_iterator_0.next();

        ((org.apache.spark.sql.execution.metric.SQLMetric) references[0] /* numOutputRows */).add(1);

        // common sub-expressions

        boolean smj_isNull_6 = smj_bufferedRow_1.isNullAt(1);
        int smj_value_8 = smj_isNull_6 ?
        -1 : (smj_bufferedRow_1.getInt(1));
        boolean smj_isNull_7 = smj_bufferedRow_1.isNullAt(2);
        Decimal smj_value_9 = smj_isNull_7 ?
        null : (smj_bufferedRow_1.getDecimal(2, 7, 2));
        smj_mutableStateArray_0[1].reset();

        smj_mutableStateArray_0[1].zeroOutNullBytes();

        if (smj_isNull_2) {
          smj_mutableStateArray_0[1].setNullAt(0);
        } else {
          smj_mutableStateArray_0[1].write(0, smj_value_4);
        }

        if (smj_isNull_3) {
          smj_mutableStateArray_0[1].setNullAt(1);
        } else {
          smj_mutableStateArray_0[1].write(1, smj_value_5);
        }

        if (smj_isNull_4) {
          smj_mutableStateArray_0[1].setNullAt(2);
        } else {
          smj_mutableStateArray_0[1].write(2, smj_value_6, 7, 2);
        }

        if (smj_isNull_6) {
          smj_mutableStateArray_0[1].setNullAt(3);
        } else {
          smj_mutableStateArray_0[1].write(3, smj_value_8);
        }

        if (smj_isNull_7) {
          smj_mutableStateArray_0[1].setNullAt(4);
        } else {
          smj_mutableStateArray_0[1].write(4, smj_value_9, 7, 2);
        }
        append((smj_mutableStateArray_0[1].getRow()).copy());

      }
      if (shouldStop()) return;
    }
    ((org.apache.spark.sql.execution.joins.SortMergeJoinExec) references[1] /* plan */).cleanupResources();
  }

}
