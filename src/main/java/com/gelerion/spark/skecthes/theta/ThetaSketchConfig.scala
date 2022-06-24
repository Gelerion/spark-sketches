package com.gelerion.spark.skecthes.theta

import com.yahoo.sketches.ResizeFactor

/**
 * @param resizeFactor see https://datasketches.github.io/docs/Theta/ThetaSize.html
 * @param nominalEntities log_base2 value, the minimum value is 2^4 and the maximum value is 2^26
 */
case class ThetaSketchConfig(resizeFactor: ResizeFactor = ResizeFactor.X2,
                             nominalEntities: Int = 4096)

