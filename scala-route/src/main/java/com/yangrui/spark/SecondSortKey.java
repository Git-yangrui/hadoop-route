package com.yangrui.spark;

import parquet.schema.Type;
import scala.math.Ordered;

import javax.print.attribute.standard.MediaSize;
import java.io.Serializable;

public class SecondSortKey implements Ordered<SecondSortKey>,Serializable {

    private int first;
    private int second;

    public SecondSortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean $less(SecondSortKey other) {
        if (this.first < other.getFirst()) {
            return true;
        } else if (this.first == other.getFirst() && this.second < other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondSortKey other) {
        if (this.first > other.getFirst()) {
            return true;
        } else if (this.first == other.getFirst() && this.first > other.getFirst()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondSortKey other) {
        if (this.$less(other)) {
            return true;
        } else if (this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondSortKey other) {
        if (this.$greater(other)) {
            return true;
        } else if (this.first == other.getFirst() && this.second == other.getSecond()) {
            return true;
        }
        return false;
    }

    @Override
    public int compare(SecondSortKey other) {
        if (this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else {
            return this.second - other.getSecond();
        }
    }


    @Override
    public int compareTo(SecondSortKey other) {
        if (this.first - other.getFirst() != 0) {
            return this.first - other.getFirst();
        } else {
            return this.second - other.getSecond();
        }
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
