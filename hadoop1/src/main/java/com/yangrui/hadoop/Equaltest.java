package com.yangrui.hadoop;

public class Equaltest {


    public Equaltest(int x, int y) {
        this.x = x;
        this.y = y;
    }

    private final int x;

    private final int y;


    @Override
    public boolean equals(Object obj) {

        if(! (obj instanceof  Equaltest)){
            return  false;
        }
        return true;

    }
}
