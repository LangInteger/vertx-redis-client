/*
 * Copyright 2019 Red Hat, Inc.
 * <p>
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * <p>
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * <p>
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 * <p>
 * You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.redis.client.impl.types;

import io.vertx.redis.client.Response;
import io.vertx.redis.client.ResponseType;

public final class NumberType implements Response {

  public static NumberType create(Number value) {
    return new NumberType(value);
  }

  private final Number value;

  private NumberType(Number value) {
    this.value = value;
  }

  @Override
  public Number toNumber() {
    return value;
  }

  @Override
  public ResponseType type() {
    return ResponseType.NUMBER;
  }

  @Override
  public Integer toInteger() {
    return value.intValue();
  }

  @Override
  public Long toLong() {
    return value.longValue();
  }

  @Override
  public String toString() {
    return value.toString();
  }
}
