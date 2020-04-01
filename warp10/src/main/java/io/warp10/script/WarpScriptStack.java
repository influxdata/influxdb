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

package io.warp10.script;

import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.StoreClient;
import io.warp10.script.functions.SNAPSHOT;
import io.warp10.script.functions.SNAPSHOT.Snapshotable;
import io.warp10.warp.sdk.WarpScriptJavaFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The WarpScript Geo Time Serie manipulation environment
 * usually uses a stack to operate.
 * 
 * There may be multiple implementations of stacks that
 * WarpScript can use, including some that persist to a
 * cache or that may spill to disk.
 * 
 * All those implementations MUST implement this interface.
 *
 */
public interface WarpScriptStack {
  
  /**
   * Signals that can be sent to a stack
   * The higher the ordinal of a signal, the higher its priority
   *
   */
  public static enum Signal {
    STOP,
    KILL,
  }
  
  public static final int DEFAULT_MAX_RECURSION_LEVEL = 16;
  public static final long DEFAULT_FETCH_LIMIT = 100000L;
  public static final long DEFAULT_GTS_LIMIT = 100000L;
  public static final long DEFAULT_MAX_OPS = 1000L;
  public static final int DEFAULT_MAX_BUCKETS = 1000000;
  public static final int DEFAULT_MAX_GEOCELLS = 10000;
  public static final int DEFAULT_MAX_DEPTH = 1000;
  public static final long DEFAULT_MAX_LOOP_DURATION = 5000L;
  public static final int DEFAULT_MAX_SYMBOLS = 64;
  public static final int DEFAULT_MAX_WEBCALLS = 4;
  public static final long DEFAULT_MAX_PIXELS = 1000000L;
  public static final long DEFAULT_MAX_JSON = 20L * 1024L * 1024L; // 20MB
  public static final long DEFAULT_REGISTERS = 256;
  
  public static final String MACRO_START = "<%";
  public static final String MACRO_END = "%>";

  public static final String COMMENT_START = "/*";
  public static final String COMMENT_END = "*/";
  
  public static final String MULTILINE_START = "<'";
  public static final String MULTILINE_END = "'>";
  
  public static final String SECURE_SCRIPT_START = "<S";
  public static final String SECURE_SCRIPT_END = "S>";
  
  public static final String TOP_LEVEL_SECTION = "[TOP]";
  
  /**
   * Name of attribute for storing macro import rules
   */
  public static final String ATTRIBUTE_IMPORT_RULES = "import.rules";
  
  /**
   * Flag indicating whether or not to set section with the current line number
   */
  public static final String ATTRIBUTE_LINENO = "lineno";
  
  /**
   * PrintWriter instance for the REL (Read Execute Loop)
   */
  public static final String ATTRIBUTE_INTERACTIVE_WRITER = "interactive.writer";
  
  /**
   * Should the REL display the stack levels as JSON?
   */
  public static final String ATTRIBUTE_INTERACTIVE_JSON = "interactive.json";
  
  /**
   * Should the interactive mode display the top of the stack after each command?
   */
  public static final String ATTRIBUTE_INTERACTIVE_ECHO = "interactive.echo";
  
  /**
   * Flag indicating whether or not to display timing information after each command.
   */
  public static final String ATTRIBUTE_INTERACTIVE_TIME = "interactive.time";
  
  /**
   * Name of current code section, null is unnamed
   */
  public static final String ATTRIBUTE_SECTION_NAME = "section.name";
  
  /**
   * Name of the current macro, or null if not in a macro or in an anonymous one
   */
  public static final String ATTRIBUTE_MACRO_NAME = "macro.name";
  
  /**
   * Flag indicating whether or not the stack is currently in documentation mode
   */
  public static final String ATTRIBUTE_DOCMODE = "docmode";
  
  /**
   * Flag indicating whether or not the stack is currently in info mode
   */
  public static final String ATTRIBUTE_INFOMODE = "infomode";
  
  /**
   * Debug depth of the stack. This is the number
   * of elements to return when an error occurs.
   */
  public static final String ATTRIBUTE_DEBUG_DEPTH = "debug.depth";
  
  /**
   * Is the stack configured to output strict JSON (i.e with no NaN/Infinity)?
   */
  public static final String ATTRIBUTE_JSON_STRICT = "json.strict";

  /**
   * Maximum size of a JSON created and pushed on the stack, in number of characters.
   */
  public static final String ATTRIBUTE_JSON_MAXSIZE = "json.size.max";
  public static final String ATTRIBUTE_JSON_MAXSIZE_HARD = "json.size.max.hard";

  /**
   * Maximum number of datapoints that can be fetched in a session
   */
  public static final String ATTRIBUTE_FETCH_LIMIT = "fetch.limit";
  public static final String ATTRIBUTE_FETCH_LIMIT_HARD = "fetch.limit.hard";

  /**
   * Maximum number of GTS which can be retrieved from directory in a session
   */
  public static final String ATTRIBUTE_GTS_LIMIT = "gts.limit";
  public static final String ATTRIBUTE_GTS_LIMIT_HARD = "gts.limit.hard";
  
  /**
   * Number of datapoints fetched so far in the session
   */
  public static final String ATTRIBUTE_FETCH_COUNT = "fetch.count";

  /**
   * Number of GTS retrieved so far in the session
   */
  public static final String ATTRIBUTE_GTS_COUNT = "gts.count";
  
  /**
   * List of elapsed times (in ns) per line
   */
  public static final String ATTRIBUTE_ELAPSED = "elapsed";
  
  /**
   * Flag indicating whether or not to track elapsed times per script line
   */
  public static final String ATTRIBUTE_TIMINGS = "timings";
  
  /**
   * Maximum duration of loops in ms and its hard limit
   */
  public static final String ATTRIBUTE_LOOP_MAXDURATION = "loop.maxduration";
  public static final String ATTRIBUTE_LOOP_MAXDURATION_HARD = "loop.maxduration.hard";
  
  /**
   * Maximum recursion depth
   */
  public static final String ATTRIBUTE_RECURSION_MAXDEPTH = "recursion.maxdepth";
  public static final String ATTRIBUTE_RECURSION_MAXDEPTH_HARD = "recursion.maxdepth.hard";
  
  /**
   * Maximum depth of the stack
   */
  public static final String ATTRIBUTE_MAX_DEPTH = "stack.maxdepth";
  public static final String ATTRIBUTE_MAX_DEPTH_HARD = "stack.maxdepth.hard";

  /**
   * Maximum number of operations for the stack
   */
  public static final String ATTRIBUTE_MAX_OPS = "stack.maxops";
  public static final String ATTRIBUTE_MAX_OPS_HARD = "stack.maxops.hard";

  /**
   * Maximum number of pixels for images created on the stack
   */
  public static final String ATTRIBUTE_MAX_PIXELS = "stack.maxpixels";
  public static final String ATTRIBUTE_MAX_PIXELS_HARD = "stack.maxpixels.hard";
  
  /**
   * Maximum number of buckets in bucketized GTS
   */
  public static final String ATTRIBUTE_MAX_BUCKETS = "stack.maxbuckets";
  public static final String ATTRIBUTE_MAX_BUCKETS_HARD = "stack.maxbuckets.hard";
  
  /**
   * Maximum number of cells if GeoXPShapes   
   */
  public static final String ATTRIBUTE_MAX_GEOCELLS = "stack.maxgeocells";
  public static final String ATTRIBUTE_MAX_GEOCELLS_HARD = "stack.maxgeocells.hard";
  
  /**
   * Current number of operations performed on this stack
   */
  public static final String ATTRIBUTE_OPS = "stack.ops";
  
  /**
   * Maximum number of symbols for the stack
   */
  public static final String ATTRIBUTE_MAX_SYMBOLS = "stack.symbols";
  public static final String ATTRIBUTE_MAX_SYMBOLS_HARD = "stack.symbols.hard";

  /**
   * Key for securing scripts
   */
  public static final String ATTRIBUTE_SECURE_KEY = "secure.key";
  
  /**
   * Flag indicating whether or not redefined functions are allowed
   */
  public static final String ATTRIBUTE_ALLOW_REDEFINED = "allow.redefined";
  
  /**
   * Key for storing an instance of Hadoop's Progressable to report progress to the Hadoop framework
   */
  public static final String ATTRIBUTE_HADOOP_PROGRESSABLE = "hadoop.progressable";
  
  /**
   * Maximum number of WEBCALL invocations per script run
   */
  public static final String ATTRIBUTE_MAX_WEBCALLS = "stack.maxwebcalls";
  
  /**
   * Token which was used to authenticate the stack, checked by some protected ops
   */
  public static final String ATTRIBUTE_TOKEN = "stack.token";
  
  /**
   * Flag indicating if we are currently in a secure macro execution
   */
  public static final String ATTRIBUTE_IN_SECURE_MACRO = "in.secure.macro";
  
  /**
   * TTL (in ms since the epoch) of a macro
   */
  public static final String ATTRIBUTE_MACRO_TTL = "macro.ttl";
  
  /**
   * List of symbols to export upon script termination as a map of symbol name
   * to symbol value pushed onto the stack.
   */
  public static final String ATTRIBUTE_EXPORTED_SYMBOLS = "exported.symbols";
  
  /**
   * Map of headers to return with the response
   */
  public static final String ATTRIBUTE_HEADERS = "response.headers";
  
  /**
   * Last error encountered in a TRY block
   */
  public static final String ATTRIBUTE_LAST_ERROR = "last.error";
  
  /**
   * Creation timestamp for the stack
   */
  public static final String ATTRIBUTE_CREATION_TIME = "creation.time";
  
  /**
   * Name given to the stack
   */  
  public static final String ATTRIBUTE_NAME = "stack.name";
  
  /**
   * Index of RETURN_DEPTH counter
   */
  public static final int COUNTER_RETURN_DEPTH = 0;
  
  public static class StackContext {}
  
  public static class Mark {}
  
  public static class Macro implements Snapshotable {
    
    /**
     * Flag indicating whether a macro is secure (its content cannot be displayed) or not
     */
    private boolean secure = false;
    
    private long fingerprint;
    
    private String name = null;
    
    /**
     * Timestamp at which the macro expired, or LONG.MIN_VALUE if no expiry date was set
     */
    private long expiry = Long.MIN_VALUE;
    
    private int size = 0;
    private Object[] statements = new Object[16];
    
    public boolean isExpired() {
      return (Long.MIN_VALUE != this.expiry) && (this.expiry < System.currentTimeMillis());
    }
    
    public String toString() {      
      return snapshot();
    }
    
    public void add(Object o) {
      ensureCapacity(1);
      statements[size++] = o;
    }
    
    public Object get(int idx) {
      return statements[idx];
    }
    
    public int size() {
      return size;
    }
    
    public void setSize(int size) {
      if (size < this.size && size >= 0) {
        this.size = size;
      }
    }

    public List<Object> statements() {
      return Arrays.asList(this.statements).subList(0, size);
    }
    
    public void addAll(Macro macro) {
      int n = macro.size;
      
      ensureCapacity(n);
      
      System.arraycopy(macro.statements, 0, this.statements, size, n);
      size += n;
    }
    
    public void setSecure(boolean secure) {
      this.secure = secure;
    }
    
    public boolean isSecure() {
      return this.secure;
    }
    
    public long getFingerprint() {
      return this.fingerprint;
    }
    
    public void setFingerprint(long fingerprint) {
      this.fingerprint = fingerprint;
    }
    
    public void setExpiry(long expiry) {
      this.expiry = expiry;
    }
    
    public void setName(String name) {
      this.name = name;
    }
    
    /**
     * Set the name of the current macro and set the same name for
     * all included macros which do not have a name
     * 
     * @param name
     */
    public void setNameRecursive(String name) {
      List<Macro> macros = new ArrayList<Macro>();
      macros.add(this);
      
      while(!macros.isEmpty()) {
        Macro m = macros.remove(0);
        if (null == m.getName()) {
          m.setName(name);
          for (Object stmt: m.statements) {
            if (stmt instanceof Macro) {
              macros.add((Macro) stmt);
            }
          }
        }
      }
    }
    
    public String getName() {
      return this.name;
    }
    
    @Override
    public String snapshot() {
      StringBuilder sb = new StringBuilder();
      
      sb.append(MACRO_START);
      sb.append(" ");

      if (!secure) {
        for (Object o: this.statements()) {
          try {
            SNAPSHOT.addElement(sb, o);
          } catch (WarpScriptException wse) {
            sb.append(WarpScriptStack.COMMENT_START);
            sb.append(" Error while snapshoting element of type '" + o.getClass() + "' ");
            sb.append(WarpScriptStack.COMMENT_END);
          }
          sb.append(" ");        
        }
      } else {
        sb.append(WarpScriptStack.COMMENT_START);
        sb.append(" Secure Macro ");
        sb.append(WarpScriptStack.COMMENT_END);
        sb.append(" ");
      }
      
      sb.append(MACRO_END);
      
      return sb.toString();
    }
    
    private void ensureCapacity(int n) {
      if (size + n < this.statements.length) {
        return;
      }
      
      int newlen = n + this.statements.length + (this.statements.length >> 1);
      this.statements = Arrays.copyOf(this.statements, newlen);
    }
  }
  
  /**
   * Retrieve the StoreClient instance associated with this stack.
   * @return 
   */
  public StoreClient getStoreClient();

  /**
   * Retrieve the DirectoryClient instance associated with this stack
   * @return
   */
  public DirectoryClient getDirectoryClient();
  
  /**
   * Push an object onto the stack
   * 
   * @param o Object to push onto the stack
   */
  public void push(Object o) throws WarpScriptException;
  
  /**
   * Remove and return the object on top of the stack.
   * 
   * @return The object on top of the stack
   * 
   * @throws InformativeEmptyStackException if the stack is empty.
   */
  public Object pop() throws InformativeEmptyStackException;
  
  /**
   * Remove and return 'N' objects from the top of the
   * stack.
   * 
   * 'N' is consumed at the top of the stack prior to
   * removing and returning the objects.
   * 
   * 
   * @return An array of 'N' objects, the first being the deepest.
   *  
   * @throws InformativeEmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException If 'N' is not present or if
   *         'N' is invalid or if the stack is not deep enough.
   */
  public Object[] popn() throws WarpScriptException;
  
  /**
   * Return the object on top of the stack without removing
   * it from the stack.
   * 
   * @return The object on top of the stack
   * 
   * @throws InformativeEmptyStackException if the stack is empty.
   */
  public Object peek() throws InformativeEmptyStackException;
  
  /**
   * Return the object at 'distance' from the top of the stack.
   * The 'distance' is on top of the stack and is consumed by 'peekn'.
   * 
   * The object on top the stack is at distance 0.
   * 
   * @throws InformativeEmptyStackException if the stack is empty.
   * @throws WarpScriptException if no valid 'distance' is on top of the stack or if the
   *         requested distance is passed the bottom of the stack.
   */
  public Object peekn() throws WarpScriptException;
  
  /**
   * Return the depth (i.e. number of objects) of the stack
   * 
   * @return The depth of the stack
   */
  public int depth();
  
  /**
   * Reset the stack to the given depth
   */
  public void reset(int depth) throws WarpScriptException;
  
  /**
   * Swap the top 2 objects of the stack.
   * 
   * @throws InformativeEmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if the stack is empty or
   *         contains a single element.
   */
  public void swap() throws WarpScriptException;
  
  /**
   * Rotate the top 3 objects of the stack, pushing
   * the top of the stack down
   * 
   *    D                     D
   *    C           ->        B
   *    B                     A
   *    A (top)               C (top)
   *    
   * @throws InformativeEmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if the stack contains less than 3 objects
   */
  public void rot() throws WarpScriptException;
  
  /**
   * Rotate up the top 'N' objects of the stack.
   * 'N' is on top of the stack and is consumed by 'roll'.
   * 
   * @throws InformativeEmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if 'N' is not present on top of the stack,
   *         is not a number or if the stack does not have enough elements for the
   *         operation.
   */
  public void roll() throws WarpScriptException;
  
  /**
   * Rotate down the top 'N' objects of the stack.
   * 'N' is on the top of the stack and is consumed by 'rolld'.
   * 
   * @throws InformativeEmptyStackException if the stack is empty
   * @throws IndexOutOfBoundsException if 'N' is not present on top of the stack,
   *         is not a number or if the stack does not have enough elements for the
   *         operation.
   */
  public void rolld() throws WarpScriptException;
  
  /**
   * Copy the object at level 'N' on top of the stack.
   * 'N' is on top of the stack and is consumed by the call to 'pick'.
   * 
   * @throws InformativeEmptyStackException
   * @throws IndexOutOfBoundsException
   */
  public void pick() throws WarpScriptException;
  
  /**
   * Remove the top of the stack.
   * 
   * @throws InformativeEmptyStackException If the stack is empty.
   */
  public void drop() throws WarpScriptException;
  
  /**
   * Remove the top 'N' objects of the stack.
   * 
   * @throws InformativeEmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException If 'N' is not present on the top of the stack,
   *         is not a number
   *         or if the stack has fewer than 'N' objects after consuming 'N'.
   */
  public void dropn() throws WarpScriptException;
  
  /**
   * Duplicate the object on top of the stack.
   * 
   * @throws InformativeEmptyStackException if the stack is empty.
   */
  public void dup() throws WarpScriptException;
  
  /**
   * Duplicate the top 'N' objects of the stack.
   * 'N' is consumed at the top of the stack first.
   * 
   * @throws InformativeEmptyStackException if the stack is empty.
   * @throws IndexOutOfBoundsException if the stack contains less than 'N' objects.
   */
  public void dupn() throws WarpScriptException;

  /**
   * Return the object at level 'level' on the stack.
   * The top of the stack is level 0
   * 
   * @param level Level of the object to return
   * @return The object found at 'level'
   * @throws WarpScriptException if the stack contains less than 'level' levels
   */
  public Object get(int level) throws WarpScriptException;
  
  public void execMulti(String script) throws WarpScriptException;
  
  /**
   * Execute a serie of statements against the stack.
   * 
   * @param line String containing a space separated list of statements to execute
   * @return
   * @throws Exception
   */
  public void exec(String line) throws WarpScriptException;
  
  /**
   * Empty the stack
   * 
   */
  public void clear();
  
  /**
   * Execute a macro against the stack.
   * 
   * @param macro Macro instance to execute
   * @return
   * @throws WarpScriptException
   */
  public void exec(Macro macro) throws WarpScriptException;

  /**
   * Execute a WarpScriptJavaFunction against the stack
   * 
   * @param function
   * @throws WarpScriptException
   */
  public void exec(WarpScriptJavaFunction function) throws WarpScriptException;

  /**
   * Find a macro by name
   * 
   * @param macroName Name of macro to find
   * @throws WarpScriptException if macro is not found
   */
  public Macro find(String macroName) throws WarpScriptException;
  
  /**
   * Execute a macro known by name.

   * @param macroName
   * @throws WarpScriptException
   */
  public void run(String macroName) throws WarpScriptException;
    
  /**
   * Produces a String representation of the top 'n' levels of the stack
   * @param n Number of stack levels to display at most
   * @return
   */
  public String dump(int n);
  
  /**
   * Return the content associated with the given symbol.
   * 
   * @param symbol Name of symbol to retrieve
   * 
   * @return The content associated with 'symbol' or null if 'symbol' is not known.
   */
  public Object load(String symbol);
  
  /**
   * Store the given object under 'symbol'.
   * 
   * @param symbol Name under which to store a value.
   * @param value Value to store.
   */
  public void store(String symbol, Object value) throws WarpScriptException;
  
  /**
   * Return the content associated with the given register
   * @param regidx Index of the register to read
   * @return The content of the given register or null if the register is not set or does not exist
   */
  public Object load(int regidx) throws WarpScriptException;
  
  /**
   * Stores the given value in register 'regidx'
   * @param regidx
   * @param value
   * @throws WarpScriptException
   */
  public void store(int regidx, Object value) throws WarpScriptException;
  
  /**
   * Forget the given symbol
   * 
   * @param symbol Name of the symbol to forget.
   */
  public void forget(String symbol);
  
  /**
   * Return the current symbol table.
   * 
   * @return
   */
  public Map<String,Object> getSymbolTable();
  
  /**
   * Return the current registers.
   * 
   * @return
   */
  public Object[] getRegisters();
  
  /**
   * Return the current map of redefined functions
   * @return
   */
  public Map<String,WarpScriptStackFunction> getDefined();
  
  /**
   * Return a UUID for the instance of WarpScriptStack
   * @return
   */
  public String getUUID();
  
  /**
   * Signal the stack, i.e. stop the currently executing code after the current statement and prevent further executions.
   */
  public void signal(Signal signal);
  
  /**
   * Throw the exception associated with the current signal sent to the stack
   */
  public void handleSignal() throws WarpScriptATCException;
  
  /**
   * Set a stack attribute.
   * 
   * @param key Key under which the attribute should be stored.
   * @param value Value of the attribute. If null, remove the attribute.
   * @return The previous value of the attribute or null if it was not set.
   */
  public Object setAttribute(String key, Object value);
  
  /**
   * Return the value of an attribute.
   * 
   * @param key Name of the attribute to retrieve.
   * 
   * @return The value associated with 'key' or null
   */
  public Object getAttribute(String key);
  
  /**
   * Return the ith counter associated with the stack
   * @param i
   * @return
   */
  public AtomicLong getCounter(int i) throws WarpScriptException;
  
  /**
   * Returns a boolean indicating whether or not the stack is authenticated.
   *
   * @return The authentication status of the stack
   */
  public boolean isAuthenticated();
  
  /**
   * Perform a final check to ensure balancing constructs are balanced.
   * 
   * @throws WarpScriptException if the stack is currently unbalanced.
   */
  public void checkBalanced() throws WarpScriptException;
  
  /**
   * (re)define 'stmt' as a valid statement executing 'macro'
   * This allows for the overriding of built-in statements
   * 
   * @param stmt
   * @param macro
   */
  public void define(String stmt, Macro macro);
  
  /**
   * Push the current stack context (symbols + redefined statements) onto the stack.
   * 
   */
  public void save() throws WarpScriptException;
  
  /**
   * Restore the stack context from that on top of the stack
   */
  public void restore() throws WarpScriptException;
  
  
}
