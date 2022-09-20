package org.apache.flink.ml.common.window;

import java.io.Serializable;

/**
 * A {@link Window} determines how input infinite data stream would be sliced into batches and fed
 * into a Flink ML Stage.
 */
public interface Window extends Serializable {}
