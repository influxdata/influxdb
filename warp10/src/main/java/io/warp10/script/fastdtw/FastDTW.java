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
/*
 * FastDTW.java   Jul 14, 2004
 *
 * Copyright (c) 2004 Stan Salvador
 * stansalvador@hotmail.com
 */

package io.warp10.script.fastdtw;

import io.warp10.continuum.gts.GeoTimeSerie;

/**
 * This class implements the FastDTW algorithm. It is derived from
 * the code found at https://github.com/cscotta/fastdtw
 * 
 */
public class FastDTW {
   // CONSTANTS
   final static int DEFAULT_SEARCH_RADIUS = 1;

   public static double getWarpDistBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ, DistanceFunction distFn) {
     return fastDTW(tsI, tsJ, DEFAULT_SEARCH_RADIUS, distFn).getDistance();
   }

   public static double getWarpDistBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ, int searchRadius, DistanceFunction distFn) {
     return fastDTW(tsI, tsJ, searchRadius, distFn).getDistance();
   }

   public static WarpPath getWarpPathBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ, DistanceFunction distFn) {
     return fastDTW(tsI, tsJ, DEFAULT_SEARCH_RADIUS, distFn).getPath();
   }

   public static WarpPath getWarpPathBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ, int searchRadius, DistanceFunction distFn) {
     return fastDTW(tsI, tsJ, searchRadius, distFn).getPath();
   }

   public static TimeWarpInfo getWarpInfoBetween(GeoTimeSerie tsI, GeoTimeSerie tsJ, int searchRadius, DistanceFunction distFn) {
     return fastDTW(tsI, tsJ, searchRadius, distFn);
   }

   private static TimeWarpInfo fastDTW(GeoTimeSerie tsI, GeoTimeSerie tsJ, int searchRadius, DistanceFunction distFn) {
     if (searchRadius < 0) {
       searchRadius = 0;
     }

     final int minTSsize = searchRadius+2;

     if ((tsI.size() <= minTSsize) || (tsJ.size()<=minTSsize)) {
       // Perform full Dynamic Time Warping.
       return DTW.getWarpInfoBetween(tsI, tsJ, distFn);
     } else {
       final double resolutionFactor = 2.0;

       //final PAA shrunkI = new PAA(tsI, (int)(tsI.size()/resolutionFactor));
       //final PAA shrunkJ = new PAA(tsJ, (int)(tsJ.size()/resolutionFactor));

// FIXME(hbs): apply PAA to tsI/tsJ
       final GeoTimeSerie shrunkI= null;
       final GeoTimeSerie shrunkJ = null;
       
       // Determine the search window that constrains the area of the cost matrix that will be evaluated based on
       //    the warp path found at the previous resolution (smaller time series).
       final SearchWindow window = new ExpandedResWindow(
           tsI,
           tsJ,
           shrunkI,
           shrunkJ,
           FastDTW.getWarpPathBetween(shrunkI, shrunkJ, searchRadius, distFn),
           searchRadius);

       // Find the optimal warp path through this search window constraint.
       return DTW.getWarpInfoBetween(tsI, tsJ, window, distFn);
     }  // end if
   }  // end recFastDTW(...)
}  // end class fastDTW
