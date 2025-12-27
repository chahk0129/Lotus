#pragma once

#include "system/global.h"
#include "utils/debug.h"
#include "index/twosided/partitioned/cache_node.h"

#if PARTITIONED
namespace partitioned{

template <class Value>
struct cache_tree_t {
    std::atomic<NodeBase*> root;

    cache_tree_t(){
		root = new BTreeLeaf<Value>();
    }

    void makeRoot(Key k, NodeBase* leftChild, NodeBase* rightChild){
		BTreeInner* inner = new BTreeInner();
		inner->count = 1;
		inner->data[0].first = k;
		inner->data[0].second = leftChild;
		inner->data[1].second = rightChild;
		root = inner;
    }

    bool insert(Key k, Value v, bool reserve=false){
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
					if(parent)
						parent->writeUnlock();
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
				if(parent)
					parent->writeUnlock();
				goto restart;
			}

			if(parent){
				parent->readUnlockOrRestart(versionParent, needRestart);
				if(needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->data[inner->lowerBound(k)].second;
			inner->checkOrRestart(versionNode, needRestart);
			if(needRestart) goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if(needRestart) goto restart;
		}

		auto leaf = static_cast<BTreeLeaf<Value>*>(node);
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
			if(needRestart)
				goto restart;

			bool ret = leaf->insert(k, v);
			node->writeUnlock();
			return ret;
		}
    }

	bool reserve(Key k, NodeBase*& temp_node, uint64_t& temp_version) {
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
					if(parent)
						parent->writeUnlock();
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
				if(parent)
					parent->writeUnlock();
				goto restart;
			}

			if(parent){
				parent->readUnlockOrRestart(versionParent, needRestart);
				if(needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->data[inner->lowerBound(k)].second;
			inner->checkOrRestart(versionNode, needRestart);
			if(needRestart) goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if(needRestart) goto restart;
		}

		auto leaf = static_cast<BTreeLeaf<Value>*>(node);
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
			if(needRestart)
				goto restart;

			bool ret = leaf->reserve(k);
			node->writeUnlock();

			temp_node = node;
			temp_version = versionNode + 0b100;
			return ret;
		}
	}

	// this uses shortcut to checkin directly for the reserved slot
	void complete_reserve(Key k, Value v, NodeBase* node, uint64_t versionNode) {
		bool needRestart = false;
		auto leaf = static_cast<BTreeLeaf<Value>*>(node);
		bool ret = false;
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if(needRestart) // version changed, restart from root
			ret = update(k, v, false);
		else {
			ret = leaf->update(k, v, false);
			leaf->writeUnlock();
		}
		assert(ret);
	}

	void undo_reserve(Key k, NodeBase* node, uint64_t versionNode) {
		bool needRestart = false;
		auto leaf = static_cast<BTreeLeaf<Value>*>(node);
		bool ret = false;
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if(needRestart) // version changed, restart from root
			ret = remove(k);
		else {
			ret = leaf->remove(k);
			leaf->writeUnlock();
		}
		assert(ret);
	}

    bool update(Key k, Value v, bool update_freq=false) {
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

		auto leaf = static_cast<BTreeLeaf<Value>*>(node);
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if(needRestart)
			goto restart;

		bool ret = leaf->update(k, v, update_freq);
		leaf->writeUnlock();
		return ret;
    }

    bool remove(Key k){
	restart:
		bool needRestart = false;
		NodeBase* node = root;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root)) goto restart;

		int idx = -1;
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

	  		idx = inner->lowerBound(k);
            node = inner->data[idx].second;
            inner->checkOrRestart(versionNode, needRestart);
            if(needRestart)
                goto restart;

            versionNode = node->readLockOrRestart(needRestart);
            if(needRestart)
                goto restart;
        }

        auto leaf = static_cast<BTreeLeaf<Value>*>(node);
        leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
        if(needRestart)
            goto restart;

        bool ret = leaf->remove(k);
        leaf->writeUnlock();
        return ret;
    }

    bool eviction_candidate(Key& k, CacheEntry<Value>*& v, NodeBase*& temp_node, uint64_t& temp_version){
	restart:
		bool ret = false;
		CacheEntry<Value>* temp_value = nullptr;

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

        auto leaf = static_cast<BTreeLeaf<Value>*>(node);
		auto idx = leaf->lowerBound(k);
		if (idx < leaf->count && leaf->count > 0) {
			k = leaf->data[idx].first;
			v = &leaf->data[idx].second;
			ret = true;
		}

		node->readUnlockOrRestart(versionNode, needRestart);
		if(needRestart)
			goto restart;
		temp_node = node;
		temp_version = versionNode;
		return ret;
	}

	bool get_lock_eviction(Key k, CacheEntry<Value>* v, NodeBase* node, uint64_t versionNode) {
		bool needRestart = false;
		v->IOLockOrRestart(needRestart);
		if (needRestart) return false; // other threads are holding IO lock on the entry

		auto leaf = static_cast<BTreeLeaf<Value>*>(node);
		leaf->checkOrRestart(versionNode, needRestart);
		if (needRestart) {
			v->IOUnlock();
			 return false; // version changed, resample the eviction candidate
		}

		return true;
	}

	void complete_eviction(Key k, NodeBase* node, uint64_t versionNode) {
		bool needRestart = false;
		bool ret = false;
		auto leaf = static_cast<BTreeLeaf<Value>*>(node);
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if(needRestart)
			ret = remove(k);
		else {
			ret = leaf->remove(k);
			leaf->writeUnlock();
		}
		assert(ret);
	}

    bool lookup(Key k, Value& v, bool update_freq=false){
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

        auto leaf = static_cast<BTreeLeaf<Value>*>(node);
		bool ret = leaf->find(k, v, update_freq);
		if(parent){
			parent->readUnlockOrRestart(versionParent, needRestart);
			if(needRestart) {
				if (update_freq) update_freq = false;
				goto restart;
			}
		}

		node->readUnlockOrRestart(versionNode, needRestart);
		if(needRestart) {
			if (update_freq) update_freq = false;
			goto restart;
		}
		return ret;
    }
};

} // namespace partitioned
#endif