#pragma once

#include "system/global.h"
#include "utils/debug.h"
#include "index/twosided/btreeolc/node.h"


template <class Value>
struct BTree{
    std::atomic<NodeBase*> root;

    BTree(){
		root = new BTreeLeaf<Value>();
    }

    BTree(Key max_key){
		root = new BTreeLeaf<Value>(max_key);
    }

    void makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild){
		BTreeInner* inner = new BTreeInner();
		inner->count = 1;
		inner->data[0].first = k;
		inner->data[0].second = leftChild;
		inner->data[1].second = rightChild;
		root = inner;
    }

	void copy(char* to, BTreeLeaf<Value>* from) { 
		memcpy(to, from, sizeof(BTreeLeaf<Value>));
		reinterpret_cast<BTreeLeaf<Value>*>(to)->reset();
	}

	bool insert(Key k, Value v){
	restart:
		bool needRestart = false;
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if(needRestart || node != root)
			goto restart;

		BTreeInner* parent = nullptr;
		uint64_t versionParent;

		while(node->type == PageType::BTreeInner){
			BTreeInner* inner = static_cast<BTreeInner*>(node);
			// Split eagerly if full
			if(inner->isFull()){
				// Lock
				if(parent){
					parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
					if(needRestart) goto restart;
				}
				node->upgradeToWriteLockOrRestart(versionNode, needRestart);
				if(needRestart){
					if(parent) parent->writeUnlock();
					goto restart;
				}

				if(!parent && (node != root)){ // there is a new parent
					node->writeUnlock();
					goto restart;
				}

				// Split
				Key sep; BTreeInner* newInner = inner->split(sep);
				if(parent)
					parent->insert(sep, newInner);
				else
					makeRoot(sep, inner, newInner);
				// Unlock and restart
				node->writeUnlock();
				if(parent) parent->writeUnlock();
				goto restart;
			}

			if(parent){
				parent->readUnlockOrRestart(versionParent, needRestart);
				if(needRestart) goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->data[inner->lowerBound(k)].second;
			inner->checkOrRestart(versionNode, needRestart);
			if(needRestart) goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if(needRestart) goto restart;
		}

		BTreeLeaf<Value>* leaf = static_cast<BTreeLeaf<Value>*>(node);
		// Split leaf if full
		if(leaf->isFull()){
			// Lock
			if(parent){
				parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
				if(needRestart) goto restart;
			}

			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if(needRestart){
				if(parent) parent->writeUnlock();
				goto restart;
			}

			if(!parent && node != root){ // there is a new parent
				node->writeUnlock();
				goto restart;
			}

			Key sep; BTreeLeaf<Value>* newLeaf = leaf->split(sep);
			if(parent)
				parent->insert(sep, newLeaf);
			else
				makeRoot(sep, leaf, newLeaf);
			// Unlock and restart
			node->writeUnlock();
			if(parent)
				parent->writeUnlock();
			goto restart;
		}
		else{
			// only lock leaf node
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if(needRestart) goto restart;

			bool ret = leaf->insert(k, v);

			node->writeUnlock();
			return ret;
		}
    }

    bool update(Key k, Value v){
	restart:
		bool needRestart = false;
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if(needRestart || node != root)
			goto restart;

		// Parent of current node
		BTreeInner* parent = nullptr;
		uint64_t versionParent;

		while(node->type == PageType::BTreeInner){
			BTreeInner* inner = static_cast<BTreeInner*>(node);
			if(parent){
				parent->readUnlockOrRestart(versionParent, needRestart);
				if(needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->data[inner->lowerBound(k)].second;
			inner->checkOrRestart(versionNode, needRestart);
			if(needRestart)
				goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if(needRestart)
				goto restart;
		}

		BTreeLeaf<Value>* leaf = static_cast<BTreeLeaf<Value>*>(node);
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if(needRestart)
			goto restart;

		bool ret = leaf->update(k, v);
		leaf->writeUnlock();
		return ret;
    }

	bool upsert(Key k, Value v, Value& old_v) {
		Value temp_v;
	restart:
		memset(&temp_v, 0, sizeof(Value));
		bool needRestart = false;
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if(needRestart || node != root)
			goto restart;

		BTreeInner* parent = nullptr;
		uint64_t versionParent;

		while(node->type == PageType::BTreeInner){
			BTreeInner* inner = static_cast<BTreeInner*>(node);
			// Split eagerly if full
			if(inner->isFull()){
				// Lock
				if(parent){
					parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
					if(needRestart) goto restart;
				}
				node->upgradeToWriteLockOrRestart(versionNode, needRestart);
				if(needRestart){
					if(parent) parent->writeUnlock();
					goto restart;
				}

				if(!parent && (node != root)){ // there is a new parent
					node->writeUnlock();
					goto restart;
				}

				// Split
				Key sep; BTreeInner* newInner = inner->split(sep);
				if(parent)
					parent->insert(sep, newInner);
				else
					makeRoot(sep, inner, newInner);
				// Unlock and restart
				node->writeUnlock();
				if(parent) parent->writeUnlock();
				goto restart;
			}

			if(parent){
				parent->readUnlockOrRestart(versionParent, needRestart);
				if(needRestart) goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->data[inner->lowerBound(k)].second;
			inner->checkOrRestart(versionNode, needRestart);
			if(needRestart) goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if(needRestart) goto restart;
		}

		BTreeLeaf<Value>* leaf = static_cast<BTreeLeaf<Value>*>(node);
		// Split leaf if full
		if(leaf->isFull()){
			// Lock
			if(parent){
				parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
				if(needRestart) goto restart;
			}

			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if(needRestart){
				if(parent) parent->writeUnlock();
				goto restart;
			}

			if(!parent && node != root){ // there is a new parent
				node->writeUnlock();
				goto restart;
			}

			Key sep; BTreeLeaf<Value>* newLeaf = leaf->split(sep);
			if(parent)
				parent->insert(sep, newLeaf);
			else
				makeRoot(sep, leaf, newLeaf);
			// Unlock and restart
			node->writeUnlock();
			if(parent)
				parent->writeUnlock();
			goto restart;
		}
		else{
			// only lock leaf node
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if(needRestart) goto restart;

			bool is_update = leaf->upsert(k, v, temp_v);

			node->writeUnlock();
			if (is_update)
				old_v = temp_v;
			return is_update;
		}
	}

	bool remove(Key k, Value v){
	restart:
		bool needRestart = false;
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root)) goto restart;

		BTreeInner* parent = nullptr;
		uint64_t versionParent;

		while(node->type == PageType::BTreeInner){
			BTreeInner* inner = static_cast<BTreeInner*>(node);
			if(parent){
				parent->readUnlockOrRestart(versionParent, needRestart);
				if(needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->data[inner->lowerBound(k)].second;
			inner->checkOrRestart(versionNode, needRestart);
			if(needRestart)
				goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if(needRestart)
				goto restart;
		}

		BTreeLeaf<Value>* leaf = static_cast<BTreeLeaf<Value>*>(node);
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if(needRestart)
			goto restart;

		bool ret = leaf->remove(k, v);
		leaf->writeUnlock();
        return ret;
    }

    bool remove(Key k){
	restart:
		bool needRestart = false;
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root)) goto restart;

		BTreeInner* parent = nullptr;
		uint64_t versionParent;

		while(node->type == PageType::BTreeInner){
			BTreeInner* inner = static_cast<BTreeInner*>(node);
			if(parent){
				parent->readUnlockOrRestart(versionParent, needRestart);
				if(needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->data[inner->lowerBound(k)].second;
			inner->checkOrRestart(versionNode, needRestart);
			if(needRestart)
				goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if(needRestart)
				goto restart;
		}

		BTreeLeaf<Value>* leaf = static_cast<BTreeLeaf<Value>*>(node);
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if(needRestart)
			goto restart;

		bool ret = leaf->remove(k);
		leaf->writeUnlock();
        return ret;
    }

    bool lookup(Key k, Value& v){
	restart:
        bool needRestart = false;
        NodeBase* node = root;
        uint64_t versionNode = node->readLockOrRestart(needRestart);
        if (needRestart || (node != root)) goto restart;

        BTreeInner* parent = nullptr;
        uint64_t versionParent;

        while(node->type == PageType::BTreeInner){
            BTreeInner* inner = static_cast<BTreeInner*>(node);
            if(parent){
                parent->readUnlockOrRestart(versionParent, needRestart);
                if(needRestart)
                    goto restart;
            }

            parent = inner;
            versionParent = versionNode;

            node = inner->data[inner->lowerBound(k)].second;
            inner->checkOrRestart(versionNode, needRestart);
            if(needRestart)
                goto restart;

            versionNode = node->readLockOrRestart(needRestart);
            if(needRestart)
                goto restart;
        }

        BTreeLeaf<Value>* leaf = static_cast<BTreeLeaf<Value>*>(node);
		bool ret = leaf->find(k, v);

		if(parent){
			parent->readUnlockOrRestart(versionParent, needRestart);
			if(needRestart) goto restart;
		}

		node->readUnlockOrRestart(versionNode, needRestart);
		if(needRestart)
			goto restart;

		return ret;
    }

	bool lowerBound(Key k, Value& v) {
	restart:
		Value temp;
		bool needRestart = false;
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root)) goto restart;

		BTreeInner* parent = nullptr;
		uint64_t versionParent;

		while(node->type == PageType::BTreeInner){
			BTreeInner* inner = static_cast<BTreeInner*>(node);
			if(parent){
				parent->readUnlockOrRestart(versionParent, needRestart);
				if(needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->data[inner->lowerBound(k)].second;
			inner->checkOrRestart(versionNode, needRestart);
			if(needRestart)
				goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if(needRestart)
				goto restart;
		}

		BTreeLeaf<Value>* leaf = static_cast<BTreeLeaf<Value>*>(node);
		auto idx = leaf->lowerBoundExist(k);
		if (idx != (unsigned)-1)
			temp = leaf->data[idx].second;
		// bool ret = leaf->find(k, v);

		if(parent){
			parent->readUnlockOrRestart(versionParent, needRestart);
			if(needRestart) goto restart;
		}

		node->readUnlockOrRestart(versionNode, needRestart);
		if(needRestart)
			goto restart;

		if (idx != (unsigned)-1) {
			v = temp;
			return true;
		}
		return false;
		// return ret;
	}

	void debug_tree(Key k) {
		NodeBase* node = root;
		while(node->type == PageType::BTreeInner){
			BTreeInner* inner = static_cast<BTreeInner*>(node);
			node = inner->data[inner->lowerBound(k)].second;
		}
		BTreeLeaf<Value>* leaf = static_cast<BTreeLeaf<Value>*>(node);
		leaf->sanity_check(k);
	}

    int scan(Key& k, int range, Value*& v){
	restart:
        bool needRestart = false;
        NodeBase* node = root;
        uint64_t versionNode = node->readLockOrRestart(needRestart);
        if (needRestart || (node != root)) goto restart;

        BTreeInner* parent = nullptr;
        uint64_t versionParent;

        while(node->type == PageType::BTreeInner){
            BTreeInner* inner = static_cast<BTreeInner*>(node);
            if(parent){
                parent->readUnlockOrRestart(versionParent, needRestart);
                if(needRestart)
                    goto restart;
            }

            parent = inner;
            versionParent = versionNode;

            node = inner->data[inner->lowerBound(k)].second;
            inner->checkOrRestart(versionNode, needRestart);
            if(needRestart)
                goto restart;

            versionNode = node->readLockOrRestart(needRestart);
            if(needRestart)
                goto restart;
        }

		int count = 0;
		auto leaf_ptr = static_cast<BTreeLeaf<Value>*>(node);
		bool needVersionCheck = false;
	restart_leaf:
		auto leaf = leaf_ptr;
		needRestart = false;
		if (needVersionCheck) { 
			versionNode = leaf->readLockOrRestart(needRestart);
			if (needRestart) goto restart_leaf;
		}
		needVersionCheck = true;

		int cnt = leaf->scan(k, range, v, count); 
		auto sibling = leaf->sibling_ptr;
		Key max_key = leaf->getMaxKey();
		leaf->readUnlockOrRestart(versionNode, needRestart);
		if(needRestart)
			goto restart_leaf;

		count += cnt;
		leaf_ptr = sibling;
		k = max_key + 1;

		if(count < range && leaf_ptr){
			goto restart_leaf;
		}

		return count;
    }
};
