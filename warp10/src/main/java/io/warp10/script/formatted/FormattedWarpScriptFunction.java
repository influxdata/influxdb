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

package io.warp10.script.formatted;

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

/**
 * Do the extraction of arguments from the stack in a formatted manner.
 * Arguments inside collections still have to be handled manually.
 *
 * Alternatively, a Map that contains all arguments and optional arguments can be provided on top of the stack.
 *
 * See FormattedWarpScriptFunctionTest for an example of child class.
 */
public abstract class FormattedWarpScriptFunction extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  private final StringBuilder docstring;
  private final List<String> unitTests;

  //
  // Argument of formatted WarpScript functions are instances of Arguments, which are built by ArgumentsBuilder
  //

  public static class Arguments {

    //
    // args: list of arguments
    // optArgs: list of optional arguments
    //

    private final List<ArgumentSpecification> args;
    private final List<ArgumentSpecification> optArgs;

    //
    // listExpandable: whether the first argument of the WarpScript function can be a list or not.
    // In that case, the result would be a list of results
    //

    private final boolean listExpandable;

    private Arguments(List<ArgumentSpecification> args, List<ArgumentSpecification> optArgs, boolean listExpandable) {
      this.args = args;
      this.optArgs = optArgs;
      this.listExpandable = listExpandable;
    }

    public List<ArgumentSpecification> getArgsCopy() {
      return new ArrayList<>(args);
    }

    public List<ArgumentSpecification> getOptArgsCopy() {
      return new ArrayList<>(optArgs);
    }

    public boolean isListExpandable() {
      return listExpandable;
    }
  }

  public static class ArgumentsBuilder {
    private final List<ArgumentSpecification> args;
    private final List<ArgumentSpecification> optArgs;
    private boolean listExpandable;

    public ArgumentsBuilder() {
      args = new ArrayList<>();
      optArgs = new ArrayList<>();
      listExpandable = false;
    }

    public ArgumentsBuilder addArgument(Class<?> clazz, String name, String doc) {
      args.add(new ArgumentSpecification(clazz, name, doc));
      return this;
    }

    public ArgumentsBuilder addListArgument(Class<?> subClazz, String name, String doc) {
      args.add(new ListSpecification(subClazz, name, doc));
      return this;
    }

    public ArgumentsBuilder addMapArgument(Class<?> keyClass, Class<?> valueClass, String name, String doc) {
      args.add(new MapSpecification(keyClass, valueClass, name, doc));
      return this;
    }

    public ArgumentsBuilder addOptionalArgument(Class<?> clazz, String name, String doc, Object defaultValue) {
      optArgs.add(new ArgumentSpecification(clazz, name, defaultValue, doc));
      return this;
    }

    public ArgumentsBuilder addOptionalListArgument(Class<?> subClazz, String name, String doc, Object defaultValue) {
      optArgs.add(new ListSpecification(subClazz, name, defaultValue, doc));
      return this;
    }

    public ArgumentsBuilder addOptionalMapArgument(Class<?> keyClass, Class<?> valueClass, String name, String doc, Object defaultValue) {
      args.add(new MapSpecification(keyClass, valueClass, name, defaultValue, doc));
      return this;
    }

    public ArgumentsBuilder addArgument(ArgumentSpecification spec) {
      args.add(spec);
      return this;
    }

    public ArgumentsBuilder addOptionalArgument(ArgumentSpecification spec) {
      if (!spec.isOptional()) {
        throw new IllegalStateException("Argument is added as an optional argument to the function but that is not coherent with its specification.");
      }

      optArgs.add(spec);
      return this;
    }

    public ArgumentsBuilder firstArgIsListExpandable() {
      listExpandable = true;
      ArgumentSpecification firstArgs = args.remove(0);
      args.add(0, new ListSpecification(firstArgs.getClazz(), firstArgs.getName(), firstArgs.getDoc()));

      return this;
    }

    public Arguments build() {
      return new Arguments(args, optArgs, listExpandable);
    }
  }

  public FormattedWarpScriptFunction(String name) {
    super(name);

    //
    // A default child class has 0 argument, 0 optional argument, an empty docstring, and 0 unit tests.
    //

    docstring = new StringBuilder();
    unitTests = new ArrayList<String>();
  }

  //
  // A child class provides the specs of its arguments by building an instance of Arguments in its constructor.
  //

  protected abstract Arguments getArguments();

  //
  // A child class uses formattedArgs to apply its function's logic, and pushes its outputs onto the stack.
  //

  protected abstract WarpScriptStack apply(Map<String, Object> formattedArgs, WarpScriptStack stack) throws WarpScriptException;

  //
  // Optionally, the child class can fill the fields docstring and unitTests for doc generation purpose.
  //

  protected final StringBuilder getDocstring() {
    return docstring;
  }

  protected List<String> getUnitTests() {
    return unitTests;
  }

  //
  // Handle arguments parsing
  //

  /**
   * Create the formatted args from the signatures provided by the child class, and apply child's apply method.
   *
   * @param stack
   * @return stack
   * @throws WarpScriptException
   */
  @Override
  final public Object apply(WarpScriptStack stack) throws WarpScriptException {

    List<ArgumentSpecification> args = getArguments().args;
    List<ArgumentSpecification> optArgs = getArguments().optArgs;

    //
    // Sanity checks
    //

    if (null == args) {
      throw new WarpScriptException(getClass().getSimpleName() + "'s method getArguments() returned null. If no " +
        "argument is expected, it should return an empty array instead.");
    }

    if (null == optArgs) {
      throw new WarpScriptException(getClass().getSimpleName() + "'s method getOptionalArguments() returned null." +
        " If no argument is expected, it should return an empty array instead.");
    }

    for (ArgumentSpecification arg: args) {
      if (arg.isOptional()) {
        throw new WarpScriptException("Output of " + getClass().getSimpleName() + "'s method getArguments() must" +
          " only contain arguments without a default value.");
      }
    }

    for (ArgumentSpecification arg: optArgs) {
      if (!arg.isOptional()) {
        throw new WarpScriptException("Output of " + getClass().getSimpleName() + "'s method getArguments() must" +
          " only contain arguments with a default value.");
      }
    }

    if (getArguments().isListExpandable() && ((ListSpecification) args.get(0)).getSubClazz() == List.class) {
      throw new WarpScriptException(getClass().getSimpleName() + " implementation error: a list argument can not be list-expandable.");
    }

    if (getArguments().isListExpandable() && args.size() == 0) {
      throw new WarpScriptException(getClass().getSimpleName() + " implementation error: a function with 0 argument can not be list-expandable.");
    }

    //
    // If args and opt args are empty, should expect nothing on top
    // If args is empty but opt args is not, should expect a map (possibly empty) on top
    //

    if (0 == args.size() && 0 == optArgs.size()) {
      return apply(new HashMap<String, Object>(), stack);
    }

    if (0 == args.size()) {

      if (0 == stack.depth() || !(stack.peek() instanceof Map)) {
        throw new WarpScriptException(getClass().getSimpleName() + " expects a MAP on top of the stack. To use default argument values, an empty MAP is expected.");
      }
    }

    //
    // Check that the first mandatory argument is not a Map so that there is no confusion
    //

    if (args.size() > 0 && Map.class.isAssignableFrom(args.get(0).getClazz())) {
      throw new WarpScriptException("The function " + getName() + " is a formatted WarpScript function. As such, it cannot expect a Map as its first mandatory argument or that could be confused for when using optional arguments. Its implementation must be modified.");
    }

    //
    // Extract arguments off the top of the stack
    //

    Map<String, Object> formattedArgs;

    if (stack.peek() instanceof Map) {

      //
      // Case 1: A map is on top of the stack (some optional arguments may be given)
      //

      for (Object o: ((Map) stack.peek()).keySet()) {
        if (!(o instanceof String)) {
          throw new WarpScriptException(getName() + "'s MAP of parameters contains a key that is not a STRING.");
        }
      }

      // use this map as function's parameters
      formattedArgs = (Map) ((HashMap<String, Object>) stack.peek()).clone();

      //
      // Check that the map does not contain unrecognized argument
      //

      for (String key: formattedArgs.keySet()) {
        boolean found = false;

        for (ArgumentSpecification arg: args) {
          if (arg.getName().equals(key)) {
            found = true;
            break;
          }
        }

        if (found) {
          break;
        }

        for (ArgumentSpecification arg: optArgs) {
          if (arg.getName().equals(key)) {
            found = true;
            break;
          }
        }

        if (!found) {
          throw new WarpScriptException("Argument '" + key + "' is not recognized by " + getName());
        }
      }

      //
      // Check that non-optional args are contained in the map and that they have the correct type
      //

      for (ArgumentSpecification arg: args) {

        Object value = formattedArgs.get(arg.getName());

        if (null == value) {

          throw new WarpScriptException("The MAP that is on top of the stack does not have the argument '" + arg.getName() +
            "' (of type "  + arg.WarpScriptType() + ") that is required by " + getName());
        }

        //
        // If first argument is list-expandable, put it in a singleton list if it is not expanded
        //

        if (getArguments().isListExpandable() && arg == args.get(0)) {

          if (!List.class.isInstance(value)) {
            Object value_ = value;
            value = new ArrayList<Object>();
            ((ArrayList<Object>) value).add(value_);

            ListSpecification firstArg = (ListSpecification) arg;
            if (!firstArg.getSubClazz().isInstance(value_)) {

              throw new WarpScriptException(getClass().getSimpleName() + " expects the argument '" + firstArg.getName() + "' to" +
                " be a " + firstArg.WarpScriptSubType() + " or a list of " + firstArg.WarpScriptSubType() + ".");
            }

            formattedArgs.put(arg.getName(), value);
          }
        }

        //
        // Check type
        //

        if (!arg.getClazz().isInstance(value)) {

          throw new WarpScriptException(getClass().getSimpleName() + " expects the argument '" + arg.getName() + "' to" +
            " be a " + arg.WarpScriptType() + ".");
        }
      }

      //
      // Check that optional args that are in the map are of the correct type
      //

      for (ArgumentSpecification arg: optArgs) {

        Object value = formattedArgs.get(arg.getName());

        if (null != value && !arg.getClazz().isInstance(value)) {

          throw new WarpScriptException(getClass().getSimpleName() + " expects the argument '" + arg.getName() + "' to " +
            "be a " + arg.WarpScriptType() + ".");
        }
      }

      //
      // Check List and Map arguments sub types
      //

      List<ArgumentSpecification> both = new ArrayList<>();
      both.addAll(args);
      both.addAll(optArgs);

      for (ArgumentSpecification arg: both) {

        Object candidate = formattedArgs.get(arg.getName());
        if (null == candidate) {
          continue;
        }

        //
        // Check lists sub types
        //

        if (arg instanceof ListSpecification) {

          for (Object elt: (List) candidate) {

            if (!((ListSpecification) arg).getSubClazz().isInstance(elt)) {
              throw new WarpScriptException(getClass().getSimpleName() + " expects the argument '" + arg.getName() + "' to be a LIST of " +
                ((ListSpecification) arg).getSubClazz().getSimpleName() + ".");
            }
          }
        }

        //
        // Check key and value sub types of maps
        //

        if (arg instanceof MapSpecification) {

          for (Object key: ((Map) candidate).keySet()) {

            if (!((MapSpecification) arg).getClassOfKey().isInstance(key)) {
              throw new WarpScriptException(getClass().getSimpleName() + " expects the argument '" + arg.getName() + "' to be a MAP with keys of type " +
                ((MapSpecification) arg).getClassOfKey().getSimpleName() + ".");
            }

            Object value = ((Map) candidate).get(key);
            if (!((MapSpecification) arg).getClassOfValue().isInstance(value)) {
              throw new WarpScriptException(getClass().getSimpleName() + " expects the argument '" + arg.getName() + "' to be a MAP with values of type " +
                ((MapSpecification) arg).getClassOfValue().getSimpleName() + ".");
            }
          }
        }
      }

      //
      // Removes the top of the stack
      //

      stack.pop();

    } else {

      //
      // Case 2: No optional argument are given
      //

      if (stack.depth() < args.size()) {
        throw new WarpScriptException(getClass().getSimpleName() + " expects to find " + args.size() + " arguments" +
          " off the top of the stack, but the stack contains only " + stack.depth() + " levels.");
      }

      //
      // If first argument is list-expandable and is not a list, replace it with a singleton list that contains it
      //

      if (getArguments().isListExpandable()) {
        Object first = stack.get(args.size() - 1);

        if (!List.class.isInstance(first)) {

          stack.push(args.size() - 1);
          Object[] cache = stack.popn();
          List<Object> arr = new ArrayList<>();
          arr.add(stack.pop());
          stack.push(arr);
          for (Object o: cache) {
            stack.push(o);
          }

          ListSpecification firstArg = (ListSpecification) args.get(0);
          if (!firstArg.getSubClazz().isInstance(first)) {

            System.out.println(first);
            System.out.println(firstArg.getSubClazz());

            throw new WarpScriptException(getClass().getSimpleName() + " expects the first argument to" +
              " be a " + firstArg.WarpScriptSubType() + " or a list of " + firstArg.WarpScriptSubType() + ".");
          }
        }
      }

      //
      // Check argument types
      //

      for (int i = 0; i < args.size(); i++) {
        ArgumentSpecification arg = args.get(args.size() - 1 - i);
        Object candidate = stack.get(i);

        if (!arg.getClazz().isInstance(candidate)) {
          throw new WarpScriptException(getClass().getSimpleName() + " expects to find a '" + arg.getName() + "' (a " +
            arg.WarpScriptType() + ") " + leveldenomination(i));
        }

        //
        // Check lists sub types
        //

        if (arg instanceof ListSpecification) {

          for (Object elt: (List) candidate) {

            if (!((ListSpecification) arg).getSubClazz().isInstance(elt)) {
              throw new WarpScriptException(getClass().getSimpleName() + " expects to find a '" + arg.getName() + "' (a LIST of " +
                ((ListSpecification) arg).getSubClazz().getSimpleName() + ") " + leveldenomination(i));
            }
          }
        }

        //
        // Check key and value sub types of maps
        //

        if (arg instanceof MapSpecification) {

          for (Object key: ((Map) candidate).keySet()) {

            if (!((MapSpecification) arg).getClassOfKey().isInstance(key)) {
              throw new WarpScriptException(getClass().getSimpleName() + " expects to find a '" + arg.getName() + "' (a MAP with keys of type " +
                ((MapSpecification) arg).getClassOfKey().getSimpleName() + ") " + leveldenomination(i));
            }

            Object value = ((Map) candidate).get(key);
            if (!((MapSpecification) arg).getClassOfValue().isInstance(value)) {
              throw new WarpScriptException(getClass().getSimpleName() + " expects to find a '" + arg.getName() + "' (a MAP with values of type " +
                ((MapSpecification) arg).getClassOfValue().getSimpleName() + ") " + leveldenomination(i));
            }
          }
        }
      }

      //
      // Consume these arguments off the top of the stack
      //

      formattedArgs = new HashMap<String,Object>();

      for (int i = 0; i < args.size(); i++) {
        ArgumentSpecification arg = args.get(args.size() - 1 - i);
        formattedArgs.put(arg.getName(), stack.pop());
      }

    }

    //
    // Set absent optional arguments to their default values
    //

    for (ArgumentSpecification arg: optArgs) {
      if (null == formattedArgs.get(arg.getName())) {
        formattedArgs.put(arg.getName(), arg.getDefaultValue());
      }
    }

    //
    // Apply child's apply
    //

    if (getArguments().isListExpandable()) {

      List<Object> firstArg = (List) formattedArgs.get(args.get(0).getName());

      if (firstArg.size() > 1) {

        int initial_depth = stack.depth();
        for (Object o : firstArg) {

          // clone arguments in case child's apply is messing with the parameter map
          Map<String, Object> subFormattedArgs = (Map) ((HashMap<String, Object>) formattedArgs).clone();

          subFormattedArgs.put(args.get(0).getName(), o);
          stack = apply(subFormattedArgs, stack);
        }

        stack.push(stack.depth() - initial_depth);
        Object[] elements = stack.popn();
        List<Object> list = new ArrayList<Object>();
        list.addAll(Arrays.asList(elements));
        stack.push(list);

        return stack;

      } else if (1 == firstArg.size()) {

        formattedArgs.put(args.get(0).getName(), firstArg.get(0));
        return apply(formattedArgs, stack);

      } else {

        // first arg list is empty so result is empty
        stack.push(new ArrayList<>());
        return stack;
      }

    }

    return apply(formattedArgs, stack);
  }

  final private String leveldenomination(int i) {
    if (i < 0) {
      throw new IllegalStateException("Can not be negative");
    }

    if (0 == i) {
      return "on top of the stack.";
    } else if (1 == i) {
      return "below the top of the stack.";
    } else if (2 == i) {
      return "on stack level 3.";
    } else {
      return "on stack level " + (i + 1) + ".";
    }
  }
}