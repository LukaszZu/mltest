package zz.test

import java.net.{InetAddress, InetSocketAddress}
import java.util

import com.datastax.driver.core.{Cluster, Host, Metadata, Session}
import com.datastax.spark.connector.cql.{CassandraConnectionFactory, CassandraConnectorConf}
import org.mockito.{ArgumentMatchers, Mockito}
import org.mockito.Mockito._

/**
  * Created by zulk on 11.04.17.
  */
object MockedCassandraFactory extends CassandraConnectionFactory {

  lazy val meta = mock(classOf[Metadata],RETURNS_MOCKS)
  lazy val cluster = mock(classOf[Cluster])
  lazy val session = mock(classOf[Session])

  override def createCluster(conf: CassandraConnectorConf): Cluster = {
    println("ŁOOOOOOOOOOOOOOO")

    val host = Mockito.mock(classOf[Host])
//    val h =new Host()

    when(host.getDatacenter).thenReturn("DC1")
    val hosts = Mockito.spy(new util.HashSet[Host]())
    hosts.add(host)

    when(hosts.contains(ArgumentMatchers.any())).thenReturn(true)

    when(meta.getClusterName).thenReturn("Cluster X")
    when(meta.getAllHosts).thenReturn(hosts)

    when(cluster.getMetadata).thenReturn(meta)
    when(cluster.connect()).thenReturn(session)

    when(session.getCluster).thenReturn(cluster)
    cluster
  }
}
