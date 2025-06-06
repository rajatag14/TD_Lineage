import json
from typing import Dict, List, Any, Optional, Tuple

# Create dynamic mock classes that your visitor can check
class MockParseContext:
    """Base mock context class"""
    pass

class MockTerminalNodeImpl:
    """Mock terminal node class"""
    pass

class MockToken:
    """Enhanced mock token with full ANTLR compatibility"""
    
    def __init__(self, index: int = 0, token_type: int = -1, text: str = "", 
                 start_index: int = 0, stop_index: int = 0, line: int = 1, 
                 column: int = 0, channel: int = 0, type_name: str = ""):
        self.index = index
        self.type = token_type
        self.text = text
        self.start_index = start_index
        self.stop_index = stop_index
        self.line = line
        self.column = column
        self.channel = channel
        self.type_name = type_name
    
    @classmethod
    def from_json(cls, token_data: Dict) -> 'MockToken':
        """Create MockToken from JSON data"""
        return cls(
            index=token_data.get('index', 0),
            token_type=token_data.get('type', -1),
            text=token_data.get('text', ''),
            start_index=token_data.get('startIndex', 0),
            stop_index=token_data.get('stopIndex', 0),
            line=token_data.get('line', 1),
            column=token_data.get('column', 0),
            channel=token_data.get('channel', 0),
            type_name=token_data.get('typeName', '')
        )
    
    # ANTLR Token interface methods
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
    
    def getChannel(self):
        return self.channel
    
    def getTokenIndex(self):
        return self.index
    
    def __str__(self):
        return f"[@{self.index},{self.start_index}:{self.stop_index}='{self.text}',<{self.type_name}>,{self.line}:{self.column}]"
    
    def __repr__(self):
        return self.__str__()


class MockTokenStream:
    """Mock token stream that behaves like ANTLR's CommonTokenStream"""
    
    def __init__(self, tokens_data: List[Dict]):
        self.tokens = [MockToken.from_json(token_data) for token_data in tokens_data]
        self._index = 0
        self._size = len(self.tokens)
    
    def get(self, index: int) -> Optional[MockToken]:
        """Get token at specific index"""
        if 0 <= index < len(self.tokens):
            return self.tokens[index]
        return None
    
    def size(self) -> int:
        """Get total number of tokens"""
        return self._size
    
    def getTokens(self, start: int = None, stop: int = None, token_types: set = None) -> List[MockToken]:
        """Get tokens in range, optionally filtered by type"""
        if start is None:
            start = 0
        if stop is None:
            stop = len(self.tokens) - 1
        
        # Ensure valid range
        start = max(0, start)
        stop = min(len(self.tokens) - 1, stop)
        
        if start > stop:
            return []
            
        result = self.tokens[start:stop+1]
        
        if token_types:
            result = [t for t in result if t.type in token_types]
            
        return result
    
    def getTokensByType(self, token_type: int) -> List[MockToken]:
        """Get all tokens of a specific type"""
        return [t for t in self.tokens if t.type == token_type]
    
    def getTokensByTypeName(self, type_name: str) -> List[MockToken]:
        """Get all tokens of a specific type name"""
        return [t for t in self.tokens if t.type_name == type_name]
    
    def getText(self, start: int = None, stop: int = None) -> str:
        """Get text representation of token range"""
        tokens_in_range = self.getTokens(start, stop)
        return ''.join(token.text for token in tokens_in_range)
    
    def getTextFromTokens(self, tokens: List[MockToken]) -> str:
        """Get text from list of tokens"""
        return ''.join(token.text for token in tokens)
    
    def seek(self, index: int):
        """Set current position in stream"""
        self._index = max(0, min(index, self._size - 1))
    
    def index(self) -> int:
        """Get current position in stream"""
        return self._index
    
    def LA(self, offset: int) -> int:
        """Look ahead - get token type at current position + offset"""
        target_index = self._index + offset - 1  # ANTLR uses 1-based offset
        if 0 <= target_index < self._size:
            return self.tokens[target_index].type
        return -1  # EOF
    
    def LT(self, offset: int) -> Optional[MockToken]:
        """Look ahead - get token at current position + offset"""
        target_index = self._index + offset - 1  # ANTLR uses 1-based offset
        if 0 <= target_index < self._size:
            return self.tokens[target_index]
        return None
    
    def consume(self):
        """Consume current token and advance position"""
        if self._index < self._size:
            self._index += 1
    
    def __len__(self):
        return self._size
    
    def __iter__(self):
        return iter(self.tokens)
    
    def __getitem__(self, index):
        return self.get(index)


class MockInterval:
    """Mock interval object for source interval compatibility"""
    
    def __init__(self, start: int, stop: int):
        self.start = start
        self.stop = stop
        self.a = start  # ANTLR uses 'a' and 'b' properties
        self.b = stop


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
        self._start_token = MockToken(
            start_index=self.start_index, 
            text=self.text,
            stop_index=self.start_index + len(self.text) - 1 if self.text else self.start_index
        )
        self._stop_token = MockToken(
            start_index=self.stop_index, 
            text=self.text,
            stop_index=self.stop_index
        )
    
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
    
    def getChild(self, i: int) -> Optional['MockParseTree']:
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


# Enhanced loading functions with token stream support

def load_tree_and_tokens_from_json(json_file_path: str, parser_rule_names: List[str] = None) -> Tuple[MockParseTree, MockTokenStream]:
    """
    Load JSON file and return both tree and token stream
    
    Args:
        json_file_path: Path to your JSON file generated by Java
        parser_rule_names: List of rule names from your parser
        
    Returns:
        tuple: (MockParseTree, MockTokenStream)
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    # Load parse tree
    tree = MockParseTree(json_data['parseTree'], parser_rule_names)
    _set_parent_references(tree)
    
    # Load token stream
    token_stream = MockTokenStream(json_data['tokenStream'])
    
    return tree, token_stream


def load_tokens_only_from_json(json_file_path: str) -> MockTokenStream:
    """
    Load only the token stream from JSON - for when you just need tokens
    
    Args:
        json_file_path: Path to your JSON file
        
    Returns:
        MockTokenStream
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    return MockTokenStream(json_data['tokenStream'])


def load_tree_from_json(json_file_path: str, parser_rule_names: List[str] = None) -> MockParseTree:
    """
    Load JSON file and return a tree object that behaves exactly like parser.parse()
    (Backward compatibility - works with both old and new JSON formats)
    
    Args:
        json_file_path: Path to your JSON file generated by Java
        parser_rule_names: List of rule names from your parser
        
    Returns:
        MockParseTree object that can be used exactly like tree = parser.parse()
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    # Handle both old format (direct tree) and new format (with parseTree key)
    if 'parseTree' in json_data:
        tree_data = json_data['parseTree']
    else:
        tree_data = json_data  # Old format compatibility
    
    tree = MockParseTree(tree_data, parser_rule_names)
    _set_parent_references(tree)
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
    
    # Handle both old format (direct tree) and new format (with parseTree key)
    if 'parseTree' in json_data:
        tree_data = json_data['parseTree']
    else:
        tree_data = json_data  # Old format compatibility
    
    tree = MockParseTree(tree_data, parser_rule_names)
    _set_parent_references(tree)
    return tree


def load_metadata_from_json(json_file_path: str) -> Dict[str, Any]:
    """
    Load only metadata from JSON file
    
    Args:
        json_file_path: Path to your JSON file
        
    Returns:
        Dictionary containing metadata
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    return json_data.get('metadata', {})


def _set_parent_references(node: MockParseTree, parent: MockParseTree = None):
    """Helper function to set parent references throughout the tree"""
    if parent is not None:
        node.setParent(parent)
    
    if node.children:
        for child in node.children:
            _set_parent_references(child, node)


# Utility functions for token analysis

def find_tokens_by_type(token_stream: MockTokenStream, type_name: str) -> List[MockToken]:
    """Find all tokens of a specific type name"""
    return token_stream.getTokensByTypeName(type_name)


def find_tokens_in_range(token_stream: MockTokenStream, start_pos: int, end_pos: int) -> List[MockToken]:
    """Find all tokens within a character position range"""
    return [token for token in token_stream.tokens 
            if token.start_index >= start_pos and token.stop_index <= end_pos]


def get_tokens_between_positions(token_stream: MockTokenStream, start_line: int, start_col: int, 
                                end_line: int, end_col: int) -> List[MockToken]:
    """Get tokens between line/column positions"""
    return [token for token in token_stream.tokens
            if (token.line > start_line or (token.line == start_line and token.column >= start_col)) and
               (token.line < end_line or (token.line == end_line and token.column <= end_col))]


def analyze_token_distribution(token_stream: MockTokenStream) -> Dict[str, int]:
    """Analyze distribution of token types"""
    distribution = {}
    for token in token_stream.tokens:
        type_name = token.type_name
        distribution[type_name] = distribution.get(type_name, 0) + 1
    return distribution
