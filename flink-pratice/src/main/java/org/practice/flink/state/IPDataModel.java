/**
 * 
 */
package org.practice.flink.state;

/**
 * @author Ramesh
 *
 */
public class IPDataModel {

	private final String userId;
	private final String networkName;
	private final String userIp;
	private final String userCountry;
	private final String website;
	private final long timeSpent;

	/**
	 * @param userId
	 * @param networkName
	 * @param userIp
	 * @param userCountry
	 * @param website
	 * @param timeSpent
	 */
	public IPDataModel(String userId, String networkName, String userIp, String userCountry, String website,
			long timeSpent) {
		super();
		this.userId = userId;
		this.networkName = networkName;
		this.userIp = userIp;
		this.userCountry = userCountry;
		this.website = website;
		this.timeSpent = timeSpent;
	}

	/**
	 * @return the userId
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * @return the networkName
	 */
	public String getNetworkName() {
		return networkName;
	}

	/**
	 * @return the userIp
	 */
	public String getUserIp() {
		return userIp;
	}

	/**
	 * @return the userCountry
	 */
	public String getUserCountry() {
		return userCountry;
	}

	/**
	 * @return the website
	 */
	public String getWebsite() {
		return website;
	}

	/**
	 * @return the timeSpent
	 */
	public long getTimeSpent() {
		return timeSpent;
	}

}
