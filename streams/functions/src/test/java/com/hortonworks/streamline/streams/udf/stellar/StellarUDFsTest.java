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

import com.hortonworks.streamline.streams.udf.stellar.StellarUDFs.*;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 */
public class StellarUDFsTest {

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  @Test
  public void testStellarStringExpression() {
    String res;
    StellarStringExpression sse = new StellarStringExpression();
    res = sse.evaluate("", "foo");
    Assert.assertEquals("foo", res);
    res = sse.evaluate(null, "foo");
    Assert.assertEquals("foo", res);
    res = sse.evaluate("JOIN([arg1, arg1], '')", "foo");
    Assert.assertEquals("foofoo", res);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStellarStringExpressionError1() {
    StellarStringExpression sse = new StellarStringExpression();
    String res = sse.evaluate("", 6);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testStellarStringExpressionError2() {
    StellarStringExpression sse = new StellarStringExpression();
    String res = sse.evaluate("[1, 2]", 6);
  }


  @Test
  public void testStellarStringExpression2() {
    String res;
    StellarStringExpression2 sse = new StellarStringExpression2();
    res = sse.evaluate("JOIN([arg1, arg2], '')", "foo", "bar");
    Assert.assertEquals("foobar", res);
  }

  @Test
  public void testStellarIntegerExpression() {
    Number res;
    StellarIntegerExpression sie = new StellarIntegerExpression();
    res = sie.evaluate("arg1 * 2", 3);
    Assert.assertEquals(6L, res);
  }

  @Test
  public void testStellarIntegerExpression2() {
    Number res;
    StellarIntegerExpression2 sie = new StellarIntegerExpression2();
    res = sie.evaluate("arg1 + arg2", 2, 3);
    Assert.assertEquals(5L, res);
  }

  @Test
  public void testStellarFloatExpression() {
    Number res;
    StellarFloatExpression sfe = new StellarFloatExpression();
    res = sfe.evaluate("arg1 * 2", 3);
    Assert.assertEquals(6.0, res);
  }

  @Test
  public void testStellarFloatExpression2() {
    Number res;
    StellarFloatExpression2 sfe = new StellarFloatExpression2();
    res = sfe.evaluate("arg1 + arg2", 2, 3);
    Assert.assertEquals(5.0, res);
  }

  @Test
  public void testStellarPredicate() {
    Boolean res;
    StellarPredicate sp = new StellarPredicate();
    res = sp.evaluate("0 < arg1", 4);
    Assert.assertEquals(true, res);
    res = sp.evaluate("0 < arg1", -4);
    Assert.assertEquals(false, res);
  }

  @Test
  public void testStellarPredicate2() {
    Boolean res;
    StellarPredicate2 sp = new StellarPredicate2();
    res = sp.evaluate("arg1 < arg2", 4, 5);
    Assert.assertEquals(true, res);
    res = sp.evaluate("arg1 < arg2", 5, 4);
    Assert.assertEquals(false, res);
  }

}
