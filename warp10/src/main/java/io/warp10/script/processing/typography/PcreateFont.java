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

package io.warp10.script.processing.typography;

import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.processing.ProcessingUtil;

import java.awt.Font;
import java.awt.FontFormatException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import processing.core.PFont;
import processing.core.PGraphics;

/**
 * Call createFont
 */
public class PcreateFont extends NamedWarpScriptFunction implements WarpScriptStackFunction {
    
  private final FontResolver resolver;
  
  private static class FontResolver {
    
    private final String warpscript;
    
    public FontResolver(String mc2) {
      this.warpscript = mc2;
    }
    
    public String resolve(String url) throws WarpScriptException {
      if (null == this.warpscript) {
        throw new WarpScriptException("Font resolver not set, rejecting all URLs.");
      }
      try {
        MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
        stack.push(url);
        stack.exec(this.warpscript);
        return (String) stack.pop();
      } catch (Exception e) {
        throw new WarpScriptException("Error resolving URL.", e);
      }
    }
  }

  public PcreateFont(String name) {
    super(name);
    
    this.resolver = new FontResolver(WarpConfig.getProperty(Configuration.PROCESSING_FONT_RESOLVER));
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    List<Object> params = ProcessingUtil.parseParams(stack, 2, 3, 4);
        
    PGraphics pg = (PGraphics) params.get(0);

    PFont font = null;
    
    //
    // Handle the case of remote font files differently, passing the URL
    // to the defined validator
    //
    
    if (params.get(1).toString().startsWith("http://")
        || params.get(1).toString().startsWith("https://")
        || params.get(1).toString().startsWith("file://")) {
      String urlstr = resolver.resolve(params.get(1).toString());

      InputStream in = null;
      
      try {
        //
        // FIXME(hbs): support WOFF/WOFF2 on top of TTF/OTF
        //
        
        // Encode the URL if it contains a whitespace
        URL url = new URL(urlstr);
        URI uri = new URI(url.getProtocol(), url.getUserInfo(), url.getHost(), url.getPort(), url.getPath(), url.getQuery(), url.getRef());
        
        in = new URL(uri.toASCIIString()).openStream(); 
        
        Font f = Font.createFont(Font.TRUETYPE_FONT, in);
        
        float size = 12.0F;
        boolean smooth = true;
        char[] charset = null;
        
        if (params.size() >= 3) {
          size = ((Number) params.get(2)).floatValue();
        }
        if (params.size() >= 4) {
          smooth = Boolean.TRUE.equals(params.get(3));
        }
        if (params.size() >= 5) {
          charset = params.get(4).toString().toCharArray();
        }
        
        font = new PFont(f.deriveFont(size * pg.parent.pixelDensity), smooth, charset, true, pg.parent.pixelDensity);
      } catch (URISyntaxException use) {
        throw new WarpScriptException(getName() + " error loading font " + urlstr, use);
      } catch (FontFormatException ffe) {
        throw new WarpScriptException(getName() + " error loading font " + urlstr, ffe);
      } catch (IOException ioe) {
        throw new WarpScriptException(getName() + " error fetching font " + urlstr, ioe);
      } finally {
        if (null != in) { try { in.close(); } catch (Exception e) {} }
      }      
    } else {
      if (3 == params.size()) {      
        font = pg.parent.createFont(params.get(1).toString(), ((Number) params.get(2)).floatValue());
      } else if (4 == params.size()) {
        font = pg.parent.createFont(
            params.get(1).toString(),
            ((Number) params.get(2)).floatValue(),
            Boolean.TRUE.equals(params.get(3)));
      } else if (5 == params.size()) {
        font = pg.parent.createFont(
            params.get(1).toString(),
            ((Number) params.get(2)).floatValue(),
            Boolean.TRUE.equals(params.get(3)),
            params.get(4).toString().toCharArray());
      }      
    }
    
    stack.push(pg);
    stack.push(font);
    
    return stack;
  }
}
