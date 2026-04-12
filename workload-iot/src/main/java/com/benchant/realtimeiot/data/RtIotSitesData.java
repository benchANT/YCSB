package com.benchant.realtimeiot.data;

public final class RtIotSitesData {

    private int siteId;
    private String address;
    private boolean isActive;
    private long constructionBegin;
    private long constructionEnd;

    public RtIotSitesData withAddress(String address) {
        this.address = address;
        return this;
    }

    public RtIotSitesData withIsActive(boolean isActive) {
        this.isActive = isActive;
        return this;
    }

    public RtIotSitesData withConstructionBegin(long constructionBegin) {
        this.constructionBegin = constructionBegin;
        return this;
    }

    public RtIotSitesData withConstructionEnd(long constructionEnd) {
        this.constructionEnd = constructionEnd;
        return this;
    }
}
