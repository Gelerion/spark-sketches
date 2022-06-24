package com.gelerion.spark.skecthes.hll

import com.yahoo.sketches.hll.{HllSketch, TgtHllType}

/**
 * @param lgConfigK  The Log2 of K for the target HLL sketch. This value must be between 4 and 21 inclusively.
 * @param tgtHllType the desired Hll type.
 */
case class HllSketchConfig(lgConfigK: Int = HllSketch.DEFAULT_LG_K,
                           tgtHllType: TgtHllType = TgtHllType.HLL_6)
