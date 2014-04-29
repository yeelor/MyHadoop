package org.robby.mr.shortestpath3;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public class Node {

	// 结点的唯一标识
	public String id;

	// 距离startNode的距离
	private int distance = Integer.MAX_VALUE;

	// 来源路径
	private String backpointer;

	// 相邻结点的结合
	private String[] adjacentNodeNames;

	// public String startName;

	// 分隔符
	public static final char fieldSeparator = '\t';

	public Node() {
	}

	public Node(String id, int distance) {
		this.id = id;
		this.distance = distance;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getDistance() {
		return distance;
	}

	public Node setDistance(int distance) {
		this.distance = distance;
		return this;
	}

	public String getBackpointer() {
		return backpointer;
	}

	public Node setBackpointer(String backpointer) {
		this.backpointer = backpointer;
		return this;
	}

	public String constructBackpointer(String name) {
		StringBuilder backpointers = new StringBuilder();
		if (StringUtils.trimToNull(getBackpointer()) != null) {
			backpointers.append(getBackpointer()).append(":");
		}
		backpointers.append(name);
		return backpointers.toString();
	}

	public String[] getAdjacentNodeNames() {
		return adjacentNodeNames;
	}

	public Node setAdjacentNodeNames(String[] adjacentNodeNames) {
		this.adjacentNodeNames = adjacentNodeNames;
		return this;
	}

	public boolean containsAjacentNodes() {
		return adjacentNodeNames != null;
	}

	public boolean isDistanceSet() {
		return distance != Integer.MAX_VALUE;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(distance).append(fieldSeparator).append(backpointer);

		if (containsAjacentNodes()) {
			sb.append(fieldSeparator).append(StringUtils.join(getAdjacentNodeNames(), fieldSeparator));
		}
		return sb.toString();
	}

	public static Node fromMR(String value) {
		String[] parts = StringUtils.splitPreserveAllTokens(value, fieldSeparator);

		Node node = new Node().setDistance(Integer.valueOf(parts[0])).setBackpointer(StringUtils.trimToNull(parts[1]));

		if (parts.length > 2) {
			node.setAdjacentNodeNames(Arrays.copyOfRange(parts, 2, parts.length));
		}
		return node;
	}
}
