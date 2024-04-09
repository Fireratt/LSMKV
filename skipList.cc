#include "skipList.h"
#include <optional>

namespace skiplist {

void skiplist_type::init()
{
    srand(time(NULL)) ; 
    head = new skiplist_node ;                      // Create the head and tail , 
                                                    // make the head's later the tail , other pointer:NULL
    tail = new skiplist_node ;
    head->later = tail ; 
    tail->prev = head ; 
    tail->key = UINT64_MAX ; 
    topHead = head ; 
    topTail = tail ; 
    n = 0 ; 
    floors = 1 ; 
}

skiplist_type::skiplist_type(double p)
{
    this->p = p ; 
    init() ; 
}

int skiplist_type::put(key_type key, const value_type &val) 
{
    skiplist_node* hand = topHead;                           // the pointer to point at the current processing node ; 
    while(hand->later)
    {
        if(!hand->later->later || hand->later->key > key)         // the right-last node or lower than right ; 
        {
            if(!hand->lower)                              // current at lowest level 
            {
                hand->insert(key, val,this->p) ;          // insert the new node and grow it according to the posibility ; 
                updateTop();                                         // set the new pointer top ; 
                n ++ ; 
                return 0; 
            }
            hand = hand-> lower ;                         // not at left-down , and met the right end (means positive inf) or lower than the right
            continue ; 
        }
        else if (hand->later->key < key)
        {
            hand = hand -> later ; 
        }
        else if (hand->later->key == key)                   // The node is in the table , change it 
        {
            hand->later->updateValue(val) ; 
            return -1 ; 
        }
    }
    return 0 ; 
}

std::string skiplist_type::get(key_type key) const 
{
    skiplist_node * hand = topHead ; 
    while(hand->later)
    {
        if(hand->later->key == key)                                     // find it 
        {
            return hand->later->val ; 
        }

        if(!hand->later->later || hand->later->key > key)         // the right-last node or lower than right ; 
        {
            if(!hand->lower)                              // didnt have lower , means it at the last level -> cant find
            {
                return "" ; 
            }
            hand = hand-> lower ;                         // not at left-down , and met the right end (means positive inf) or lower than the right
            continue ; 
        }
        else if (hand->later->key < key)
        {
            hand = hand -> later ; 
        }
    }
    return NULL ;                                        // control will never reach here ; 
}
int skiplist_type::query_distance(key_type key) const 
{
    skiplist_node * hand = topHead ; 
    int steps = 0 ; 
    while(hand->later)
    {
        if(!hand->later->later || hand->later->key > key)         // the right-last node or lower than right ; 
        {
            if(!hand->lower)                              // didnt have lower , means it at the last level -> cant find
            {
                return -1; 
            }
            hand = hand-> lower ;                         // not at left-down , and met the right end (means positive inf) or lower than the right
            steps ++ ; 
            continue ; 
        }
        if(hand->later->key == key)                                     // find it 
        {
            // printf("Key:%ld STEPS:%d\n",key,steps) ; 
            return steps ; 
        }
        if (hand->later->key < key)
        {
            hand = hand -> later ; 
            steps ++ ; 
        }
    }
    // printf("Key:%ld STEPS:%d\n",key,steps) ; 
    return steps ; 
}

skiplist_node* skiplist_type::arrive(key_type key) const
{

    skiplist_node * hand = topHead ; 
    while(hand->later)
    {
        if(hand->later->key == key)                                     // find it 
        {
            // to the bottom of the key 
            skiplist_node * columnHand = hand->later; 
            while(columnHand->lower)
            {
                columnHand = columnHand->lower ; 
            }

            return columnHand ; 
        }

        if(!hand->later->later || hand->later->key > key)         // the right-last node or lower than right ; 
        {
            if(!hand->lower)                              // didnt have lower , means it at the last level -> cant find
            {
                return hand ; 
            }
            hand = hand-> lower ;                         // not at left-down , and met the right end (means positive inf) or lower than the right
            continue ; 
        }
        else if (hand->later->key < key)
        {
            hand = hand -> later ; 
        }
    }
    return NULL ;                                        // control will never reach here ; 
}

void skiplist_node::insert(key_type key, const value_type &val , double p , skiplist_node * root)
{
    skiplist_node * hand = this ; 
    insert(key,val,root) ; 
    double posibleCounter = p * 10000;                                // use the quantity of situations to replace the float posibility；
    int randomNum = rand() ;  
    // printf("randomNum::%d\n",randomNum) ; 
    if((randomNum%10000) < posibleCounter)                             // cast the coin to decide if needed to grow . 
    {
        // find where to insert ; 
        while(!hand->upper)
        {
            if(!hand->prev)                                         // arrive at the left end and top level 
            {
                hand->upper = new skiplist_node ; 
                hand->upper->later = new skiplist_node ; 
                hand->upper->lower = hand ; 
                hand->upper->later->prev = hand->upper ; 
                hand->upper->insert(key,val,p,this->later) ; 
                return ; 
            }
            hand = hand->prev ;
        }
        hand->upper->insert(key,val,p,this->later) ;                            // recursively insert it ;  
    }
}

void skiplist_node::insert(key_type key, const value_type &val,skiplist_node * root)
{
    // printf("INSERT!") ; 
    skiplist_node * tem = later ; 
    skiplist_node * newNode = new skiplist_node(key,val,this,tem , NULL , root) ; 
    this->later = newNode ; 
    tem->prev = newNode ; 
    if(root)
        root->upper = newNode ; 

}
skiplist_node::skiplist_node(
	key_type key , const value_type& val ,
	skiplist_node * prev  , skiplist_node * later  ,
	skiplist_node * upper  , skiplist_node * lower  
) : val(val) 
{
    this->key = key ; 
    this->prev = (prev) ; this->later = (later) ; this->upper = (upper) ; this->lower = (lower) ; 
}

void skiplist_node::updateValue(const value_type &val)
{
    this->val = std::string(val) ; 
    skiplist_node * hand = this ; 

    while(hand->upper)
    {
        hand->upper->val = std::string(val) ;  
        hand = hand->upper ; 
    }
    skiplist_node * hand2 = this ; 
    while(hand2->lower)
    {
        hand2->lower->val = std::string(val) ;  
        hand2 = hand2->lower ; 

    }
}

void skiplist_type::updateTop()     
{
    // Set the top floor to log 1/p n 
    double maxHeight = log2(n)/log2(1.0/p) ; 
    while(topHead->upper 
    && floors < (int)maxHeight )
    {
        topHead = topHead->upper ; 
        floors++ ; 
    }
    // printf("Current Floor:%d\n",floors) ; 
}
void skiplist_type::PRINT_STATUS() const
{
    const char * splitLine = "#####" ; 
    printf("%s Start to Print STATUS of SKIPLIST %s\n",splitLine,splitLine) ; 
    printf("Floors: %d ; ELements Numbers:  %d\n" , floors , n) ; 
    // Start print the elements in the floor
    skiplist_node * hand = this->topHead ; 
    while(hand)
    {
        skiplist_node * rowHand = hand->later ; 
        printf("HEAD") ; 
        while(rowHand->later)
        {
            printf("-> %ld : %s ", rowHand->key , rowHand->val.c_str()) ; 
            rowHand = rowHand->later ; 
        }
        printf("->TAIL\n") ; 
        hand = hand->lower ; 
    }
    printf("%s Print End %s\n\n",splitLine,splitLine); 
}

int skiplist_type::size() const
{
    return n ; 
}

skiplist_node * skiplist_type::min() const 
{
    return head->later ; 
}

skiplist_node * skiplist_type::max() const 
{
    return tail->prev ; 
}

void skiplist_type::reset() 
{
    skiplist_node * bottomHand = head->later ; 
    while(bottomHand->later)                    // tail have no later node . 
    {
        skiplist_node * upHand = bottomHand ; 
        skiplist_node * next = bottomHand->later ; 
        while(upHand)
        {
            skiplist_node* tem = upHand->upper ; 
            delete upHand ; 
            upHand = tem ; 
        }
        bottomHand = next ; 
    }
    // remove the tail and head's children nodes
    skiplist_node * headHand = head->upper ; 
    while(headHand)
    {
        delete headHand ; 
        headHand = headHand->upper ; 
    }
    skiplist_node * tailHand = tail->upper ; 
    while(tailHand)
    {
        delete tailHand ; 
        tailHand = tailHand->upper ; 
    }
    // reset the tail and head status 
    head->upper = NULL ; 
    tail->upper = NULL ; 
    head->later = tail ; 
    tail->prev = head ; 
    // reset n , and floors status
    floors = 1 ; 
    n = 0 ; 
    // reset topHead topTail
    topHead = head ; 
    topTail = tail ; 
}





} // namespace skiplist

