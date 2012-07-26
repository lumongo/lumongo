package org.lumongo.server.searching;

import java.util.Comparator;

import org.lumongo.cluster.message.Lumongo.ScoredResult;

public class ScoreCompare implements Comparator<ScoredResult> {
    @Override
    public int compare(ScoredResult o1, ScoredResult o2) {
        int compare = Double.compare(o1.getScore(), o2.getScore());
        if (compare == 0) {
            return Integer.compare(o1.getResultIndex(), o2.getResultIndex());
        }
        return compare;
    }
}


