import java.util.*
import java.nio.charset.StandardCharsets

import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*

import dev.diogenes.hbase.connector.*
import dev.diogenes.hbase.connector.utils.*

connector = new HBaseConnector(new Properties(), System.getenv())
fetcher = new HBaseStreamFetcher(connector)
