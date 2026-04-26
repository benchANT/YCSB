package com.benchant.realtimeiot.data;

public final class RtIotSitesData {

    private int siteId;
    private String address;
    private boolean isActive;
    private long constructionBegin;
    private long constructionEnd;

    public int getSiteId() {
        return siteId;
    }

    public String getAddress() {
        return address;
    }

    public boolean isActive() {
        return isActive;
    }

    public long getConstructionBegin() {
        return constructionBegin;
    }

    public long getConstructionEnd() {
        return constructionEnd;
    }

    public RtIotSitesData withSiteId(int siteId) {
        this.siteId = siteId;
        return this;
    }

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

    @Override
    public String toString() {
        return "RtIotSitesData{" +
                "siteId=" + siteId +
                ", address='" + address + '\'' +
                ", isActive=" + isActive +
                ", constructionBegin=" + constructionBegin +
                ", constructionEnd=" + constructionEnd +
                '}';
    }
}