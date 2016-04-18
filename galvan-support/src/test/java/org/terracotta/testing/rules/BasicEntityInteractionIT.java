/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.testing.rules;

import java.io.File;
import java.util.Arrays;
import java.util.UUID;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.testing.demos.TestHelpers;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.terracotta.entity.barrier.Barrier;
import org.terracotta.entity.barrier.BarrierConfig;
import org.terracotta.entity.barrier.TerracottaBarrierServerEntity;
import org.terracotta.entity.echo.EchoCodec;

public class BasicEntityInteractionIT {

  public static String createEntityName() {
    return "testEntity-";// + System.currentTimeMillis();
  }

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("target/cluster"), 1,
          Arrays.asList(
                  new File(TestHelpers.jarContainingClass(TerracottaBarrierServerEntity.class)),
                  new File(TestHelpers.jarContainingClass(BarrierConfig.class)),
                  new File(TestHelpers.jarContainingClass(EchoCodec.class))), "", "");

  @Test
  public void testAbsentEntityRetrievalFails() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<Barrier, BarrierConfig> ref = client.getEntityRef(Barrier.class, 1, createEntityName());

      try {
        ref.fetchEntity();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testAbsentEntityCreationSucceeds() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<Barrier, BarrierConfig> ref = client.getEntityRef(Barrier.class, 1, createEntityName());

      ref.create(new BarrierConfig(1));
      try {
        Barrier entity = ref.fetchEntity();
        try {
          assertThat(entity.getParties(), is(1));
        } finally {
          entity.close();
        }
      } finally {
        ref.destroy();
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testPresentEntityCreationFails() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<Barrier, BarrierConfig> ref = client.getEntityRef(Barrier.class, 1, createEntityName());

      ref.create(new BarrierConfig(1));
      try {
        try {
          ref.create(new BarrierConfig(1));
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }

        try {
          ref.create(new BarrierConfig(2));
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }
      } finally {
        ref.destroy();
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testAbsentEntityDestroyFails() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<Barrier, BarrierConfig> ref = client.getEntityRef(Barrier.class, 1, createEntityName());

      try {
        ref.destroy();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testPresentEntityDestroySucceeds() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<Barrier, BarrierConfig> ref = client.getEntityRef(Barrier.class, 1, createEntityName());

      ref.create(new BarrierConfig(1));
      ref.destroy();

      try {
        ref.fetchEntity();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    } finally {
      client.close();
    }
  }

  @Test
  @Ignore
  public void testPresentEntityDestroyBlockedByHeldReferenceSucceeds() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<Barrier, BarrierConfig> ref = client.getEntityRef(Barrier.class, 1, createEntityName());

      ref.create(new BarrierConfig(1));

      Barrier entity = ref.fetchEntity();
      try {
        ref.destroy();
      } finally {
        entity.close();
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void testPresentEntityDestroyNotBlockedByReleasedReferenceSucceeds() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<Barrier, BarrierConfig> ref = client.getEntityRef(Barrier.class, 1, createEntityName());

      ref.create(new BarrierConfig(1));
      ref.fetchEntity().close();
      ref.destroy();
    } finally {
      client.close();
    }
  }

  @Test
  public void testDestroyedEntityAllowsRecreation() throws Throwable {
    Connection client = CLUSTER.newConnection();
    try {
      EntityRef<Barrier, BarrierConfig> ref = client.getEntityRef(Barrier.class, 1, createEntityName());

      ref.create(new BarrierConfig(2));
      ref.destroy();

      UUID uuid = UUID.randomUUID();
      ref.create(new BarrierConfig(1));
      try {
        Barrier entity = ref.fetchEntity();
        try {
          assertThat(entity.getParties(), is(1));
        } finally {
          entity.close();
        }
      } finally {
        ref.destroy();
      }
    } finally {
      client.close();
    }
  }
}
