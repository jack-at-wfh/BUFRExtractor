package com.weathernexus.utilities.bufr.readers

import com.weathernexus.utilities.bufr.data.{BufrCodeFlagKey, BufrCodeFlagEntry}


/**
 * A type-safe reader for querying an aggregated map of BUFR code flags.
 * * @param data The immutable map of aggregated data.
 */

case class BufrCodeFlagReader(data: Map[BufrCodeFlagKey, List[BufrCodeFlagEntry]])
  extends Reader[BufrCodeFlagKey, BufrCodeFlagEntry]