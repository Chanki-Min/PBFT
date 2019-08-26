package kr.ac.hongik.apl.Blockchain;

import kr.ac.hongik.apl.Util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static kr.ac.hongik.apl.Util.hash;

public class HashTree implements Serializable {
	Node root = null;

	public HashTree(List<Serializable> objectList) {
		this(objectList.stream().map(Util::hash).toArray(String[]::new));
	}

	public HashTree(String[] hashList) {
		List<Node> leafs = Arrays.stream(hashList)
				.map(Node::new)
				.collect(Collectors.toList());
		this.root = buildTree(leafs);
	}

	private Node buildTree(List<Node> siblings) {
		if (siblings.size() == 1)
			return siblings.get(0);

		List<Node> parents = new ArrayList<>();
		for (int i = 0; i < siblings.size(); i += 2) {
			try {
				String leftHash = siblings.get(i).getHash(),
						rightHash = siblings.get(i + 1).getHash(),
						parentsHash = hash(leftHash.concat(rightHash));
				Node parent = new Node(parentsHash);
				parent.left = siblings.get(i);
				parent.right = siblings.get(i + 1);
				parents.add(parent);
			} catch (IndexOutOfBoundsException e) {
				Node parent = new Node(siblings.get(i).getHash());
				parent.left = siblings.get(i);
				parent.right = null;
				parents.add(parent);
			}
		}

		return buildTree(parents);
	}

	public boolean verifyFrom(List<Serializable> objectList) {
		String[] newHashList = objectList.stream()
				.map(Util::hash)
				.toArray(String[]::new);
		HashTree newTree = new HashTree(newHashList);
		return this.root.getHash().equals(newTree.root.getHash());
	}

	@Override
	public String toString() {
		return root.getHash();
	}
}
