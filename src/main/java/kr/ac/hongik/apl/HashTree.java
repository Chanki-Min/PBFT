package kr.ac.hongik.apl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static kr.ac.hongik.apl.Util.hash;

public class HashTree implements Serializable {
	Node root = null;

	public HashTree(List<String> hashList) {
		List<Node> leafs = hashList.stream()
				.map(Node::new)
				.collect(Collectors.toList());
		this.root = buildTree(leafs);
	}

	Node buildTree(List<Node> siblings) {
		if (siblings.size() == 1)
			return siblings.get(0);

		List<Node> parents = new ArrayList<>();
		try {
			for (int i = 0; i < siblings.size(); i += 2) {
				String leftHash = siblings.get(i).getHash(),
						rightHash = siblings.get(i + 1).getHash(),
						parentsHash = hash(leftHash.concat(rightHash));
				Node parent = new Node(parentsHash);
				parent.left = siblings.get(i);
				parent.right = siblings.get(i + 1);
				parents.add(parent);
			}
		} catch (IndexOutOfBoundsException e) {
			throw new IllegalArgumentException("List size must be 2^n");
		}

		return buildTree(parents);
	}

	public boolean verifyFrom(List<Serializable> objectList) {
		List<String> newHashList = objectList.stream()
				.map(Util::hash)
				.collect(Collectors.toList());
		HashTree newTree = new HashTree(newHashList);
		return this.root.getHash().equals(newTree.root.getHash());
	}

	@Override
	public String toString() {
		return root.getHash();
	}
}
