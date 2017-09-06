package com.yangrui.spark

import com.ibeifeng.sparkproject.util.Constants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.util.AccumulatorV2

class SessionAccumulator extends AccumulatorV2[String, String] {

  private var initValue = Constants.SESSION_COUNT + "=0|" +
    Constants.TIME_PERIOD_1s_3s +
    "=0|" + Constants.TIME_PERIOD_4s_6s +
    "=0|" + Constants.TIME_PERIOD_7s_9s +
    "=0|" + Constants.TIME_PERIOD_10s_30s +
    "=0|" + Constants.TIME_PERIOD_30s_60s +
    "=0|" + Constants.TIME_PERIOD_1m_3m +
    "=0|" + Constants.TIME_PERIOD_3m_10m +
    "=0|" + Constants.TIME_PERIOD_10m_30m +
    "=0|" + Constants.TIME_PERIOD_30m +
    "=0|" + Constants.STEP_PERIOD_1_3 +
    "=0|" + Constants.STEP_PERIOD_4_6 +
    "=0|" + Constants.STEP_PERIOD_7_9 +
    "=0|" + Constants.STEP_PERIOD_10_30 +
    "=0|" + Constants.STEP_PERIOD_30_60 +
    "=0|" + Constants.STEP_PERIOD_60 + "=0"

  override def isZero: Boolean = true

  override def copy(): AccumulatorV2[String, String] = {
    val sessionAccumulator = new SessionAccumulator
    sessionAccumulator.initValue = this.initValue
    sessionAccumulator
  }

  override def reset(): Unit = {
    initValue = Constants.TIME_PERIOD_1s_3s +
      "=0|" + Constants.TIME_PERIOD_4s_6s +
      "=0|" + Constants.TIME_PERIOD_7s_9s +
      "=0|" + Constants.TIME_PERIOD_10s_30s +
      "=0|" + Constants.TIME_PERIOD_30s_60s +
      "=0|" + Constants.TIME_PERIOD_1m_3m +
      "=0|" + Constants.TIME_PERIOD_3m_10m +
      "=0|" + Constants.TIME_PERIOD_10m_30m +
      "=0|" + Constants.TIME_PERIOD_30m +
      "=0|" + Constants.STEP_PERIOD_1_3 +
      "=0|" + Constants.STEP_PERIOD_4_6 +
      "=0|" + Constants.STEP_PERIOD_7_9 +
      "=0|" + Constants.STEP_PERIOD_10_30 +
      "=0|" + Constants.STEP_PERIOD_30_60 +
      "=0|" + Constants.STEP_PERIOD_60 + "=0"
  }

  override def add(v: String): Unit = {
    val v1 = initValue
    val v2 = v
    if (StringUtils.isNotEmpty(v1) && StringUtils.isNotEmpty(v2)) {
      var newResult = ""
      val oldValue = com.ibeifeng.sparkproject.util.StringUtils.getFieldFromConcatString(v1, "\\|", v2)
      if (oldValue != null) {
        val newValue = String.valueOf(Integer.valueOf(oldValue) + 1)
        newResult = com.ibeifeng.sparkproject.util.StringUtils.setFieldInConcatString(v1, "\\|", v2, newValue)
      }
      initValue = newResult
    }


  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {

    if (other != null) {
      initValue=other.value
    }

  }

  override def value: String = initValue
}
