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

package io.warp10.script.functions;

import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.script.ElementOrListStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;

/**
 * Apply chunk on GTS or GTSEncoder instances
 * <p>
 * CHUNK expects the following parameters on the stack:
 * <p>
 * 5: lastchunk end Timestamp of the most recent chunk to create
 * 4: chunkwidth Width of each chunk in time units
 * 3: chunkcount Number of chunks GTS to produce
 * 2: chunklabel Name of the label containing the id of the chunk
 * 1: keepempty Should we keep empty chunks
 */
public class CHUNK extends ElementOrListStackFunction {
  private final boolean withOverlap;

  public CHUNK(String name, boolean withOverlap) {
    super(name);
    this.withOverlap = withOverlap;
  }

  @Override
  public ElementStackFunction generateFunction(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    if (!(top instanceof Boolean)) {
      throw new WarpScriptException(getName() + " expects on top of the stack a boolean indicating whether or not to keep empty chunks.");
    }

    final boolean keepEmpty = (boolean) top;

    top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects the name of the 'chunk' label below the top of the stack.");
    }

    final String chunkLabel = (String) top;

    top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a number of chunks under the 'chunk' label.");
    }

    final long chunkCount = (long) top;

    final long overlap;

    if (this.withOverlap) {
      top = stack.pop();
      if (!(top instanceof Long)) {
        throw new WarpScriptException(getName() + " expects an overlap below the number of chunks.");
      }
      overlap = Math.max(0L, (long) top);
    } else {
      overlap = 0L;
    }

    top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects a chunk width on top of the end timestamp of the most recent chunk.");
    }

    final long chunkWidth = (long) top;

    top = stack.pop();

    if (!(top instanceof Long)) {
      throw new WarpScriptException(getName() + " expects the end timestamp of the most recent chunk under the chunk width.");
    }

    final long lastChunk = (long) top;

    return new ElementStackFunction() {
      @Override
      public Object applyOnElement(Object element) throws WarpScriptException {
        if (element instanceof GeoTimeSerie) {
          return GTSHelper.chunk((GeoTimeSerie) element, lastChunk, chunkWidth, chunkCount, chunkLabel, keepEmpty, overlap);
        } else if (element instanceof GTSEncoder) {
          return GTSHelper.chunk((GTSEncoder) element, lastChunk, chunkWidth, chunkCount, chunkLabel, keepEmpty, overlap);
        } else {
          throw new WarpScriptException(getName() + " expects a Geo Time Series, a GTSEncoder or a list thereof under the lastchunk parameter.");
        }
      }
    };
  }
}
