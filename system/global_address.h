#pragma once
#include <cstdint>

class global_addr_t{
public:
	union{
		struct { // for DEFT 
			uint64_t cl_ver : 4;  // cache line version
			uint64_t node_version : 4; // for child node in ptr
			uint64_t read_gran : 4; // for child node in ptr
			uint64_t nodeID : 4;
			uint64_t offset : 48;
		}; 
	    struct{ // for DEX && Sherman
			uint64_t node_id : 16;
			uint64_t addr : 48;
	    };
	    uint64_t val;
	};

	global_addr_t(): node_id(0), addr(0){ }
	global_addr_t(uint64_t val): val(val){ }
	global_addr_t(uint64_t node_id, uint64_t addr): node_id(node_id), addr(addr){ }
	global_addr_t(const global_addr_t& other): val(other.val){ }

	operator uint64_t() { return val; }

	static global_addr_t null(){
	    return global_addr_t();
	}

	inline bool operator==(const global_addr_t& other){
	    return (node_id == other.node_id) && (addr == other.addr);
	}

	inline bool operator!=(const global_addr_t& other){
	    return !(*this == other);
	}

	inline global_addr_t& operator=(const global_addr_t& other){
		val = other.val;
	    // node_id = other.node_id;
	    // addr = other.addr;
	    return *this;
	}

	// inline global_addr_t& operator=(const global_addr_t& other){
	// 	uint8_t ver = cl_ver;
	// 	val = other.val;
	// 	cl_ver = ver;
	// 	return *this;
	// }
/*	
	inline bool operator==(const global_addr_t& other) {
		return (nodeID == other.nodeID) && (offset == other.offset);
	}

	inline bool operator!=(const global_addr_t& other) {
		return !(*this == other);
	}
*/
	// inline global_addr_t operator+(const global_addr_t& other){
	//     addr += other.addr;
	//     return *this;
	// }

	// inline global_addr_t operator+(const uint64_t& other){
	//     addr += other;
	//     return *this;
	// }

	bool is_null() {
	    return (*this) == this->null();
	}

} __attribute__((packed));

inline global_addr_t GADD(const global_addr_t& other, int off) {
	auto ret = other;
	ret.offset += off;
	return ret;
}

#ifndef PARTITIONED // deft
inline bool operator==(const global_addr_t &lhs, const global_addr_t &rhs) {
  	return (lhs.nodeID == rhs.nodeID) && (lhs.offset == rhs.offset);
}

inline bool operator!=(const global_addr_t &lhs, const global_addr_t &rhs) {
  	return !(lhs == rhs);
}

inline std::ostream &operator<<(std::ostream &os, const global_addr_t &obj) {
	os << "[" << (int)obj.nodeID << ", " << obj.offset << "]";
  	return os;
}
#endif