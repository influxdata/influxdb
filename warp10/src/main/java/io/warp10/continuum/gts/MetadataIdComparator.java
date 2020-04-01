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

import io.warp10.continuum.store.thrift.data.Metadata;

import java.util.Comparator;

/**
 *Sort metadatas by increasing classId/labelsId byte representation to speed up reseeks
 *
 *@see ID_COMPARATOR in Directory
 */
public class MetadataIdComparator implements Comparator<Metadata> {
  
  public static final MetadataIdComparator COMPARATOR = new MetadataIdComparator();
  
  private MetadataIdComparator() {}
  
  @Override
  public int compare(Metadata meta1, Metadata meta2) {
    
    long cls1 = meta1.getClassId();
    long cls2 = meta2.getClassId();
    
    if (cls1 == cls2) {
      // Class ids are identical, compare label ids
      long lbls1 = meta1.getLabelsId();
      long lbls2 = meta2.getLabelsId();
      
      if (lbls1 == lbls2) {
        // Labels id are identical, return 0
        return 0;
      } else {
        if ((lbls1 & 0x8000000000000000L) == (lbls2 & 0x8000000000000000L)) {
          // lbls1 and lbls2 have the same sign
          if ((lbls1 & 0x8000000000000000L) == 0) {
            if (lbls1 < lbls2) {
              return -1;
            } else {
              return 1;
            }
          } else {
            if ((lbls1 ^ 0x8000000000000000L) < (lbls2 ^ 0x8000000000000000L)) {
              return -1;
            } else {
              return 1;
            }
          }
        } else {
          // lbls1 and lbls2 have different sign, the smallest is the positive one
          if ((lbls1 & 0x8000000000000000L) == 0) {
            return -1;
          } else {
            return 1;
          }
        }
      }
    } else {
      if ((cls1 & 0x8000000000000000L) == (cls2 & 0x8000000000000000L)) {
        // cls1 and cls2 have the same sign
        if ((cls1 & 0x8000000000000000L) == 0) {
          if (cls1 < cls2) {
            return -1;
          } else {
            return 1;
          }
        } else {
          if ((cls1 ^ 0x8000000000000000L) < (cls2 ^ 0x8000000000000000L)) {
            return -1;
          } else {
            return 1;
          }
        }
      } else {
        // cls1 and cls2 have different sign, the smallest is the positive one
        if ((cls1 & 0x8000000000000000L) == 0) {
          return -1;
        } else {
          return 1;
        }
      }
    }
  }
}
