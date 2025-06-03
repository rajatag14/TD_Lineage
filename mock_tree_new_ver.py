import json
from typing import Dict, List, Any, Optional

# Create dynamic mock classes that your visitor can check
class MockParseContext:
    """Base mock context class"""
    pass

class MockTerminalNodeImpl:
    """Mock terminal node class"""
    pass

class MockParseTree:
    """
    Mock ParseTree object that behaves exactly like ANTLR's ParseTree.
    This replaces the tree returned by parser.parse() but reads from JSON.
    Enhanced to properly handle class assignment and visitor patterns.
    """
    
    def __new__(cls, json_node: Dict[str, Any], parser_rule_names: List[str] = None):
        """
        Custom __new__ to properly create the right class instance from the start.
        This ensures the object is created as the correct dynamic class type.
        """
        # Extract basic properties to determine class type
        node_type = json_node.get('nodeType', '')
        is_rule_context = node_type.startswith('RuleContext: ')
        is_terminal = node_type.startswith('Terminal: ')
        
        if is_rule_context:
            rule_name = node_type.replace('RuleContext: ', '')
            class_name = f"{rule_name}Context"
            # Create dynamic class for this rule context
            dynamic_class = type(class_name, (MockParseTree, MockParseContext), {
                '_rule_name': rule_name,
                '_class_name': class_name,
                '_is_rule_context': True,
                '_is_terminal': False
            })
        elif is_terminal:
            token_name = node_type.replace('Terminal: ', '')
            class_name = "TerminalNodeImpl"
            dynamic_class = type(class_name, (MockParseTree, MockTerminalNodeImpl), {
                '_token_name': token_name,
                '_class_name': class_name,
                '_is_rule_context': False,
                '_is_terminal': True
            })
        else:
            rule_name = 'unknown'
            class_name = "UnknownContext"
            dynamic_class = type(class_name, (MockParseTree, MockParseContext), {
                '_rule_name': rule_name,
                '_class_name': class_name,
                '_is_rule_context': True,
                '_is_terminal': False
            })
        
        # Create instance of the dynamic class
        instance = object.__new__(dynamic_class)
        return instance
    
    def __init__(self, json_node: Dict[str, Any], parser_rule_names: List[str] = None):
        """
        Initialize with JSON node data.
        
        Args:
            json_node: Dictionary from JSON representing this tree node
            parser_rule_names: List of rule names from your parser
        """
        self.json_data = json_node
        self.rule_names = parser_rule_names or []
        self._children_cache = None
        
        # Extract basic properties
        self.node_type = json_node.get('nodeType', '')
        self.text = json_node.get('text', '')
        self.start_index = json_node.get('startIndex', -1)
        self.stop_index = json_node.get('stopIndex', -1)
        
        # Use class attributes set by __new__
        self.is_rule_context = getattr(self.__class__, '_is_rule_context', False)
        self.is_terminal = getattr(self.__class__, '_is_terminal', False)
        self.rule_name = getattr(self.__class__, '_rule_name', 'unknown')
        self.token_name = getattr(self.__class__, '_token_name', 'unknown')
        self.class_name = getattr(self.__class__, '_class_name', 'UnknownContext')
        
        # Add ANTLR-compatible attributes that your visitor might expect
        self.exception = None  # ParserRuleContext has this
        self.invokingState = -1  # ParserRuleContext has this
        
        # Create start and stop tokens
        self._start_token = MockToken(self.start_index, self.text)
        self._stop_token = MockToken(self.stop_index, self.text)
    
    @property
    def children(self) -> Optional[List['MockParseTree']]:
        """Return children list - exactly like ANTLR ParseTree"""
        if self.getChildCount() == 0:
            return None  # Match ANTLR behavior - returns None when no children
        
        if self._children_cache is None:
            self._children_cache = [self.getChild(i) for i in range(self.getChildCount())]
        return self._children_cache
    
    @property 
    def start(self):
        """Mock start token - ParserRuleContext property"""
        return self._start_token
    
    @property
    def stop(self):
        """Mock stop token - ParserRuleContext property"""  
        return self._stop_token
    
    def getChildCount(self) -> int:
        """Return number of children - exactly like ANTLR ParseTree"""
        children = self.json_data.get('children', [])
        return len(children)
    
    def getChild(self, i: int) -> 'MockParseTree':
        """Get child at index i - exactly like ANTLR ParseTree"""
        children = self.json_data.get('children', [])
        if i < 0 or i >= len(children):
            return None
        return MockParseTree(children[i], self.rule_names)
    
    def getText(self) -> str:
        """Get text content - exactly like ANTLR ParseTree"""
        return self.text
    
    def getRuleIndex(self) -> int:
        """Get rule index for rule contexts - exactly like ANTLR ParserRuleContext"""
        if not self.is_rule_context:
            return -1
        
        try:
            return self.rule_names.index(self.rule_name)
        except (ValueError, AttributeError):
            return -1
    
    def getSourceInterval(self):
        """Mock source interval for compatibility"""
        return MockInterval(self.start_index, self.stop_index)
    
    def getStart(self):
        """Get start token - ParserRuleContext method"""
        return self.start
    
    def getStop(self):
        """Get stop token - ParserRuleContext method"""
        return self.stop
    
    def accept(self, visitor):
        """Accept visitor pattern - delegates to visitor methods"""
        if self.is_rule_context:
            # Try exact rule name method first (visitParse, visitSelectStatement, etc.)
            method_name = f'visit{self.rule_name}'
            if hasattr(visitor, method_name):
                method = getattr(visitor, method_name)
                return method(self)
            
            # Try with 'Context' suffix removed if it exists
            clean_rule_name = self.rule_name.replace('Context', '')
            method_name = f'visit{clean_rule_name}'
            if hasattr(visitor, method_name):
                method = getattr(visitor, method_name)
                return method(self)
        
        # Fall back to visitChildren
        if hasattr(visitor, 'visitChildren'):
            return visitor.visitChildren(self)
        else:
            return self.visitChildren(visitor)
    
    def visitChildren(self, visitor):
        """Visit all children with the visitor"""
        result = None
        for i in range(self.getChildCount()):
            child = self.getChild(i)
            if child:
                child_result = child.accept(visitor)
                if child_result is not None:
                    result = child_result
        return result
    
    def toStringTree(self, rule_names: List[str] = None) -> str:
        """Generate string representation of tree for debugging"""
        if self.is_terminal:
            return f'"{self.getText()}"'
        
        if self.getChildCount() == 0:
            return f'({self.rule_name})'
        
        children_str = ' '.join(
            self.getChild(i).toStringTree(rule_names) 
            for i in range(self.getChildCount())
        )
        return f'({self.rule_name} {children_str})'
    
    # Make it behave like the specific context types your code expects
    def __getattr__(self, name):
        """
        Dynamic attribute access for context-specific methods.
        If your code calls methods like node.selectStatement() or node.IDENTIFIER(),
        this will try to find child nodes matching those patterns.
        """
        # Handle rule-specific method calls
        if name.endswith('_list') or name.endswith('List'):
            # For methods that return lists of child nodes
            base_name = name.replace('_list', '').replace('List', '')
            return [child for child in self._get_all_children() 
                   if getattr(child, 'rule_name', '') == base_name]
        
        # For single child node access
        for child in self._get_all_children():
            if hasattr(child, 'rule_name') and child.rule_name == name:
                return child
            if hasattr(child, 'token_name') and child.token_name == name:
                return child
        
        # If not found, return None (like ANTLR does)
        return None
    
    def _get_all_children(self):
        """Get all children as MockParseTree objects"""
        if self.children is None:
            return []
        return self.children
    
    # Add methods that might be called by your visitor
    def getPayload(self):
        """Get payload - RuleNode method"""
        return self
    
    def getParent(self):
        """Get parent - would need to be set during tree construction"""
        return getattr(self, '_parent', None)
    
    def setParent(self, parent):
        """Set parent node"""
        self._parent = parent
    
    def __str__(self):
        rule_or_token = getattr(self, 'rule_name', None) or getattr(self, 'token_name', 'unknown')
        return f"Mock{self.__class__.__name__}[{rule_or_token}]: {self.text}"
    
    def __repr__(self):
        return self.__str__()
    
    # Debug method to see what this object looks like
    def debug_info(self):
        """Debug method to see object structure"""
        return {
            'class_name': self.__class__.__name__,
            'rule_name': getattr(self, 'rule_name', None),
            'token_name': getattr(self, 'token_name', None),
            'is_rule_context': self.is_rule_context,
            'is_terminal': self.is_terminal,
            'text': self.text,
            'child_count': self.getChildCount(),
            'attributes': [attr for attr in dir(self) if not attr.startswith('_')]
        }


class MockToken:
    """Mock token object for compatibility with ANTLR Token interface"""
    
    def __init__(self, index: int, text: str):
        self.index = index
        self.text = text
        self.start_index = index
        self.stop_index = index + len(text) - 1 if text else index
        self.type = -1  # Token type
        self.line = 1
        self.column = 0
    
    def getStartIndex(self):
        return self.start_index
    
    def getStopIndex(self):
        return self.stop_index
    
    def getText(self):
        return self.text
    
    def getType(self):
        return self.type
    
    def getLine(self):
        return self.line
    
    def getCharPositionInLine(self):
        return self.column


class MockInterval:
    """Mock interval object for source interval compatibility"""
    
    def __init__(self, start: int, stop: int):
        self.start = start
        self.stop = stop
        self.a = start  # ANTLR uses 'a' and 'b' properties
        self.b = stop


def load_tree_from_json(json_file_path: str, parser_rule_names: List[str] = None) -> MockParseTree:
    """
    Load JSON file and return a tree object that behaves exactly like parser.parse()
    
    Args:
        json_file_path: Path to your JSON file generated by Java
        parser_rule_names: List of rule names from your parser
        
    Returns:
        MockParseTree object that can be used exactly like tree = parser.parse()
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    tree = MockParseTree(json_data, parser_rule_names)
    _set_parent_references(tree)  # Set parent references for proper tree structure
    return tree


def load_tree_from_json_string(json_string: str, parser_rule_names: List[str] = None) -> MockParseTree:
    """
    Load JSON string and return a tree object that behaves exactly like parser.parse()
    
    Args:
        json_string: JSON string from your Java output
        parser_rule_names: List of rule names from your parser
        
    Returns:
        MockParseTree object that can be used exactly like tree = parser.parse()
    """
    json_data = json.loads(json_string)
    tree = MockParseTree(json_data, parser_rule_names)
    _set_parent_references(tree)  # Set parent references for proper tree structure
    return tree


def _set_parent_references(node: MockParseTree, parent: MockParseTree = None):
    """Helper function to set parent references throughout the tree"""
    if parent is not None:
        node.setParent(parent)
    
    if node.children:
        for child in node.children:
            _set_parent_references(child, node)
