/**
 * Copyright 2017 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.hortonworks.streamline.streams.udf.stellar;

import com.hortonworks.streamline.streams.rule.UDF2;
import com.hortonworks.streamline.streams.rule.UDF3;
import org.apache.metron.stellar.common.StellarProcessor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.MapVariableResolver;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.metron.stellar.dsl.VariableResolver;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is a container for the basic Stellar UDFs, each of which is implemented as a
 * static public nested class: StellarStringExpression, StellarStringExpression2,
 * StellarNumericExpression, StellarNumericExpression2, StellarPredicate, StellarPredicate2.
 */
public class StellarUDFs {

  static public class StellarStringExpression implements UDF2<String, String, Object> {
    transient StellarProcessor stellarProcessor = new StellarProcessor();

    @Override
    public String evaluate(String stellarFunction, Object arg1) {
      // If no stellarFunction is provided, just act as identity function if possible
      if(stellarFunction == null || stellarFunction.trim().isEmpty()) {
        if (arg1 != null && arg1 instanceof String) {return (String)arg1;}
        else throw new IllegalArgumentException("empty Stellar function with non-string argument is not allowed. Arg: " + arg1);
      }

      Map inputs = new HashMap<String, Object>();
      inputs.put("arg1", arg1);

      VariableResolver vResolver = new MapVariableResolver(inputs);
      Object objResult = stellarProcessor.parse(stellarFunction, vResolver, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
      if(objResult != null && !(objResult instanceof String)) {
        throw new IllegalArgumentException("Stellar Function <" + stellarFunction + "> did not return a String value. Returned: " + objResult);
      }
      return objResult == null ? "" : (String)objResult;
    }
  }

  static public class StellarStringExpression2 implements UDF3<String, String, Object, Object> {
    transient StellarProcessor stellarProcessor = new StellarProcessor();

    @Override
    public String evaluate(String stellarFunction, Object arg1, Object arg2) {
      // If no stellarFunction is provided, just act as identity function if possible
      if(stellarFunction == null || stellarFunction.trim().isEmpty()) {
        if (arg1 != null && arg1 instanceof String) {return (String)arg1;}
        else throw new IllegalArgumentException("empty Stellar function with non-string first argument is not allowed. Arg: " + arg1);
      }

      Map inputs = new HashMap<String, Object>();
      inputs.put("arg1", arg1);
      inputs.put("arg2", arg2);

      VariableResolver vResolver = new MapVariableResolver(inputs);
      Object objResult = stellarProcessor.parse(stellarFunction, vResolver, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
      if(objResult != null && !(objResult instanceof String)) {
        throw new IllegalArgumentException("Stellar Function <" + stellarFunction + "> did not return a String value. Returned: " + objResult);
      }
      return objResult == null ? "" : (String)objResult;
    }
  }

  static public class StellarIntegerExpression implements UDF2<Long, String, Object> {
    transient StellarProcessor stellarProcessor = new StellarProcessor();

    @Override
    public Long evaluate(String stellarFunction, Object arg1) {
      // If no stellarFunction is provided, just act as identity function if possible
      if(stellarFunction == null || stellarFunction.trim().isEmpty()) {
        if (arg1 != null && arg1 instanceof Number) {return ((Number) arg1).longValue();}
        else throw new IllegalArgumentException("empty Stellar function with non-numeric argument is not allowed. Arg: " + arg1);
      }

      Map inputs = new HashMap<String, Object>();
      inputs.put("arg1", arg1);

      VariableResolver vResolver = new MapVariableResolver(inputs);
      Object objResult = stellarProcessor.parse(stellarFunction, vResolver, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
      if(objResult == null || !(objResult instanceof Number)) {
        throw new IllegalArgumentException("Stellar Function <" + stellarFunction + "> did not return a Numeric value. Returned: " + objResult);
      }
      return ((Number) objResult).longValue();
    }
  }

  static public class StellarIntegerExpression2 implements UDF3<Long, String, Object, Object> {
    transient StellarProcessor stellarProcessor = new StellarProcessor();

    @Override
    public Long evaluate(String stellarFunction, Object arg1, Object arg2) {
      // If no stellarFunction is provided, just act as identity function if possible
      if(stellarFunction == null || stellarFunction.trim().isEmpty()) {
        if (arg1 != null && arg1 instanceof Number) {return ((Number) arg1).longValue();}
        else throw new IllegalArgumentException("empty Stellar function with non-numeric first argument is not allowed. Arg: " + arg1);
      }

      Map inputs = new HashMap<String, Object>();
      inputs.put("arg1", arg1);
      inputs.put("arg2", arg2);

      VariableResolver vResolver = new MapVariableResolver(inputs);
      Object objResult = stellarProcessor.parse(stellarFunction, vResolver, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
      if(objResult == null || !(objResult instanceof Number)) {
        throw new IllegalArgumentException("Stellar Function <" + stellarFunction + "> did not return a Numeric value. Returned: " + objResult);
      }
      return ((Number) objResult).longValue();
    }
  }

  static public class StellarFloatExpression implements UDF2<Double, String, Object> {
    transient StellarProcessor stellarProcessor = new StellarProcessor();

    @Override
    public Double evaluate(String stellarFunction, Object arg1) {
      // If no stellarFunction is provided, just act as identity function if possible
      if(stellarFunction == null || stellarFunction.trim().isEmpty()) {
        if (arg1 != null && arg1 instanceof Number) {return ((Number) arg1).doubleValue();}
        else throw new IllegalArgumentException("empty Stellar function with non-numeric argument is not allowed. Arg: " + arg1);
      }

      Map inputs = new HashMap<String, Object>();
      inputs.put("arg1", arg1);

      VariableResolver vResolver = new MapVariableResolver(inputs);
      Object objResult = stellarProcessor.parse(stellarFunction, vResolver, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
      if(objResult == null || !(objResult instanceof Number)) {
        throw new IllegalArgumentException("Stellar Function <" + stellarFunction + "> did not return a numeric value. Returned: " + objResult);
      }
      return ((Number) objResult).doubleValue();
    }
  }

  static public class StellarFloatExpression2 implements UDF3<Double, String, Object, Object> {
    transient StellarProcessor stellarProcessor = new StellarProcessor();

    @Override
    public Double evaluate(String stellarFunction, Object arg1, Object arg2) {
      // If no stellarFunction is provided, just act as identity function if possible
      if(stellarFunction == null || stellarFunction.trim().isEmpty()) {
        if (arg1 != null && arg1 instanceof Number) {return ((Number) arg1).doubleValue();}
        else throw new IllegalArgumentException("empty Stellar function with non-numeric first argument is not allowed. Arg: " + arg1);
      }

      Map inputs = new HashMap<String, Object>();
      inputs.put("arg1", arg1);
      inputs.put("arg2", arg2);

      VariableResolver vResolver = new MapVariableResolver(inputs);
      Object objResult = stellarProcessor.parse(stellarFunction, vResolver, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
      if(objResult == null || !(objResult instanceof Number)) {
        throw new IllegalArgumentException("Stellar Function <" + stellarFunction + "> did not return a numeric value. Returned: " + objResult);
      }
      return ((Number) objResult).doubleValue();
    }
  }

  static public class StellarPredicate implements UDF2<Boolean, String, Object> {
    transient StellarProcessor stellarProcessor = new StellarProcessor();

    @Override
    public Boolean evaluate(String stellarFunction, Object arg1) {
      // If no stellarFunction is provided, no meaningful identity relationship is likely
      if(stellarFunction == null || stellarFunction.trim().isEmpty()) {
        throw new IllegalArgumentException("empty Stellar predicate is not allowed. Arg: " + arg1);
      }

      Map inputs = new HashMap<String, Object>();
      inputs.put("arg1", arg1);

      VariableResolver vResolver = new MapVariableResolver(inputs);
      Object objResult = stellarProcessor.parse(stellarFunction, vResolver, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
      if(objResult == null || !(objResult instanceof Boolean)) {
        throw new IllegalArgumentException("Stellar Function <" + stellarFunction + "> did not return a Boolean value. Returned: " + objResult);
      }
      return (Boolean)objResult;
    }
  }

  static public class StellarPredicate2 implements UDF3<Boolean, String, Object, Object> {
    transient StellarProcessor stellarProcessor = new StellarProcessor();

    @Override
    public Boolean evaluate(String stellarFunction, Object arg1, Object arg2) {
      // If no stellarFunction is provided, no meaningful identity relationship is likely
      if(stellarFunction == null || stellarFunction.trim().isEmpty()) {
        throw new IllegalArgumentException("empty Stellar predicate is not allowed. Arg: " + arg1);
      }

      Map inputs = new HashMap<String, Object>();
      inputs.put("arg1", arg1);
      inputs.put("arg2", arg2);

      VariableResolver vResolver = new MapVariableResolver(inputs);
      Object objResult = stellarProcessor.parse(stellarFunction, vResolver, StellarFunctions.FUNCTION_RESOLVER(), Context.EMPTY_CONTEXT());
      if(objResult == null || !(objResult instanceof Boolean)) {
        throw new IllegalArgumentException("Stellar Function <" + stellarFunction + "> did not return a Boolean value. Returned: " + objResult);
      }
      return (Boolean)objResult;
    }
  }

}
