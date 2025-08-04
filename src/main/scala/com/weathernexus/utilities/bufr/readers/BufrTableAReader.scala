package com.weathernexus.utilities.bufr.readers

import com.weathernexus.utilities.bufr.data.{BufrTableAKey, BufrTableAEntry}


/**
 * A type-safe reader for querying an aggregated map of BUFR Table A entries.
 * @param data The immutable map of aggregated data.
 */

case class BufrTableAReader(data: Map[BufrTableAKey, List[BufrTableAEntry]])
  extends Reader[BufrTableAKey, BufrTableAEntry]