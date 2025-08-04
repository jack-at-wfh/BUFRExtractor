package com.weathernexus.utilities.bufr.readers

import com.weathernexus.utilities.bufr.data.{BufrTableBKey, BufrTableBEntry}


/**
 * A type-safe reader for querying an aggregated map of BUFR Table B entries.
 * @param data The immutable map of aggregated data.
 */

case class BufrTableBReader(data: Map[BufrTableBKey, List[BufrTableBEntry]])
  extends Reader[BufrTableBKey, BufrTableBEntry]