//
//   Copyright 2019  SenX S.A.S.
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

import java.util.Comparator;

public class GTSIdComparator implements Comparator<GeoTimeSerie> {

  public static final GTSIdComparator COMPARATOR = new GTSIdComparator();
  
  private GTSIdComparator() {}

  @Override
  public int compare(GeoTimeSerie o1, GeoTimeSerie o2) {
    return MetadataIdComparator.COMPARATOR.compare(o1.getMetadata(), o2.getMetadata());
  }
}
