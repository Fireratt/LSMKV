#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cstdint>
// #include <optional>
// #include <vector>
#include <string>
#include <cmath>
namespace skiplist {
using key_type = uint64_t;
// using value_type = std::vector<char>;
using value_type = std::string;

class skiplist_node
{
	public:
		key_type key ; 
		value_type val ; 
		skiplist_node * prev ; 						// The left 
		skiplist_node * later ; 					// The right 
		skiplist_node * upper ; 					// the up ; 
		skiplist_node * lower ; 					// the lower ; 
		skiplist_node(
			key_type key = 0 , const value_type& val = "" ,
			skiplist_node * prev = NULL , skiplist_node * later = NULL ,
			skiplist_node * upper = NULL , skiplist_node * lower = NULL 
		) ; 
		// return new top's location ; 
		void insert(key_type key, const value_type &val , double p , skiplist_node * root = NULL) ; 		// insert the new node and grow it according to the posibility ; 
		void insert(key_type key, const value_type &val , skiplist_node * root) ; 					// directly insert behind current node ; 
		// update the value of all the nodes 
        void updateValue(const value_type &val) ; 

};

class skiplist_type
{
	// add something here
	private:
		int floors ; 
        int n ; 
		skiplist_node * head ; 						// The first List of the whole skiplist ; It will not contain any value ; 
		skiplist_node * tail ; 
		skiplist_node * topHead ; 
		skiplist_node * topTail ; 
		void init() ; 								// initialize the whole skiplist , initialize the head and tail ... ;
		double p ; 									// the posibility to grow the list . 
		void updateTop() ; 							// update the tophead(entrance)
	public:
		explicit skiplist_type(double p = 0.5);
		int put(key_type key, const value_type &val);   // return 0 : dont have the key , insert . return -1 : have the key ,overwrite
		//std::optional<value_type> get(key_type key) const;
		std::string get(key_type key) const;
		// for hw1 only
		int query_distance(key_type key) const;
        // print the skipList's all the status(include the ele from top to end , the number of ele and floors...)
        void PRINT_STATUS() const ;          
		// according the key , find the node(at the bottom) . if cant find , return it's left node's addr at the bottom . 
		skiplist_node* arrive(key_type key) const;
		// get current ele Number
		int size() const ; 
		// get current minimum
		skiplist_node* min() const ; 
		// get current maximum
		skiplist_node* max() const ; 
		// reset the whole skipList 
		void reset() ; 
		// check if skipList is empty 
		bool isEmpty() ; 
		// return the head 
		skiplist_node * getHead() const ; 
		// return the topHead
		skiplist_node * getTopHead() const;
		
};	

} // namespace skiplist

#endif // SKIPLIST_H
