class Stack:
  
    #define the constructor
    def __init__(self):
        #We will initiate the stack with an empty list
        self.items = []
        
    def is_empty(self):
        """
        Returns whether the stack is empty or not
        Runs in constant time O(1) as does not depend on size
        """
        #return True if empty
        #Return False if not
        return not self.items
      
    def push(self, item):
        """Adds item to the end of the Stack, returns Nothing
        Runs in constant time O(1) as no indices are change
        as we add to the right of the list
        """
        #use standard list method append
        self.items.append(item)
        
    def pop(self):
        """Removes the last item from the Stack and returns it
        Runs in constant time O(1) as only an item is removed,
        no indices are changed
        """
        #as long as there are items in the list
        #then return the list item
        if self.items:
            #use standard list method pop()
            #removes the end in pyhton >3.6
            return self.items.pop()
        #otherwise return None
        else:
            return None
          
    def peek(self):
        """Return the final value from the Stack
        if there is a final value
        Runs in constant time O(1) as only finding the
        end of the list using the index
        """
        #if there are items in the Stack
        if self.items:
            #then return the item
            return self.items[-1]
       #otherwise return None
        else:
            return None

    def setLast(self, item):
        # if there are items in the Stack
        if self.items:
            # then set the item on top of the stack
            self.items[-1] = item
        # otherwise return None
        else:
            return None
          
    def size(self):
        """Return the size of the Stack
        Runs in constant time O(1) as only checks the size"""
        #will return 0 if empty
        #so no need to check
        return len(self.items)
      
    def __str__(self):
        """Return a string representation of the Stack"""
        return str(self.items)