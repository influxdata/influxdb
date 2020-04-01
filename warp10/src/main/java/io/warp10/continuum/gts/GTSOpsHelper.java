//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.continuum.gts;

public class GTSOpsHelper {
  public static interface GTSBinaryOp {
    public Object op(GeoTimeSerie gtsa, GeoTimeSerie gtsb, int idxa, int idxb);
  }

  public static void handleBucketization(GeoTimeSerie result, GeoTimeSerie gts1, GeoTimeSerie gts2) {
    if (GTSHelper.isBucketized(gts1) && GTSHelper.isBucketized(gts2)) {
      if (GTSHelper.getBucketSpan(gts1) == GTSHelper.getBucketSpan(gts2)) {
        // Both GTS have the same bucket span, check their lastbucket to see if they have the
        // same remainder modulo the bucketspan
        long bucketspan = GTSHelper.getBucketSpan(gts1);
        if (GTSHelper.getLastBucket(gts1) % bucketspan == GTSHelper.getLastBucket(gts2) % bucketspan) {
          GTSHelper.setBucketSpan(result, bucketspan);
          GTSHelper.setLastBucket(result, Math.max(GTSHelper.getLastBucket(gts1), GTSHelper.getLastBucket(gts2)));
          // Compute the number of bucket counts
          long firstbucket = Math.min(GTSHelper.getLastBucket(gts1) - (GTSHelper.getBucketCount(gts1) - 1) * bucketspan, GTSHelper.getLastBucket(gts2) - (GTSHelper.getBucketCount(gts2) - 1) * bucketspan);
          int bucketcount = (int) ((GTSHelper.getLastBucket(result) - firstbucket) / bucketspan) + 1;
          GTSHelper.setBucketCount(result, bucketcount);
        }
      }
    }
  }

  public static void applyBinaryOp(GeoTimeSerie result, GeoTimeSerie gts1, GeoTimeSerie gts2, GTSBinaryOp op) {
    applyBinaryOp(result, gts1, gts2, op,false);
  }
  public static void applyBinaryOp(GeoTimeSerie result, GeoTimeSerie gts1, GeoTimeSerie gts2, GTSBinaryOp op, boolean copyGts1Location) {
    // Determine if result should be bucketized or not
    handleBucketization(result, gts1, gts2);
    
    // Sort GTS
    GTSHelper.sort(gts1);
    GTSHelper.sort(gts2);
    
    // Sweeping line over the timestamps
    int idxa = 0;
    int idxb = 0;
    
    int na = GTSHelper.nvalues(gts1);
    int nb = GTSHelper.nvalues(gts2);
    
    Long tsa = null;
    Long tsb = null;

    if (idxa < na) {
      tsa = GTSHelper.tickAtIndex(gts1, idxa);
    }
    if (idxb < na) {
      tsb = GTSHelper.tickAtIndex(gts2, idxb);
    }
    
    while(idxa < na || idxb < nb) {
      if (idxa >= na) {
        tsa = null;
      }
      if (idxb >= nb) {
        tsb = null;
      }
      if (null != tsa && null != tsb) {
        // We have values at the current index for both GTS
        if (0 == tsa.compareTo(tsb)) {
          // Both indices indicate the same timestamp
          if (copyGts1Location) {
            GTSHelper.setValue(result, tsa,GTSHelper.locationAtIndex(gts1,idxa),GTSHelper.elevationAtIndex(gts1,idxa),op.op(gts1, gts2, idxa, idxb),false);
          } else {
            GTSHelper.setValue(result, tsa, op.op(gts1, gts2, idxa, idxb));
          }
          // Advance both indices
          idxa++;
          idxb++;
        } else if (tsa < tsb) {
          // Timestamp at index A is lower than timestamp at index B
          // Advance index for GTS A
          idxa++;
        } else {
          // Timestamp at index B is >= timestamp at index B
          // Advance index for GTS B
          idxb++;
        }
      } else if (null == tsa && null != tsb) {
        // Index A has reached the end of GTS A, GTS B still has values to scan
        idxb++;
      } else if (null == tsb && null != tsa) {
        // Index B has reached the end of GTS B, GTS A still has values to scan
        idxa++;
      }
      if (idxa < na) {
        tsa = GTSHelper.tickAtIndex(gts1, idxa);
      }
      if (idxb < nb) {
        tsb = GTSHelper.tickAtIndex(gts2, idxb);
      }
    }
  }
}
