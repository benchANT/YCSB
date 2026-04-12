package com.benchant.realtimeiot.data;

public final class RtIotOperatorData {

    private long operatorId;
    private String firstName;
    private String lastName;

    private String getOperatorName() {
        return firstName + " " + lastName;
    }

    @Override
    public String toString() {
        return "RtIotOperatorData{" +
            "operatorId=" + operatorId +
            ", operatorName='" + getOperatorName() + '\'' +
            '}';
    }

    public RtIotOperatorData withOperatorId(long operatorId) {
      this.operatorId = operatorId;
      return this;
    }

    public RtIotOperatorData withFirstName(String firstName) {
      this.firstName = firstName;
      return this;
    }

    public RtIotOperatorData withLastName(String lastName) {
      this.lastName = lastName;
      return this;
    }
}