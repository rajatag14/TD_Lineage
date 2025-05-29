import json
from typing import Dict, List, Any, Optional

class MockParseTree:
    """
    Mock ParseTree object that behaves exactly like ANTLR's ParseTree.
    This replaces the tree returned by parser.parse() but reads from JSON.
    """
    
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
        
        # Determine if this is a rule context or terminal
        self.is_rule_context = self.node_type.startswith('RuleContext: ')
        self.is_terminal = self.node_type.startswith('Terminal: ')
        
        # Extract rule/token name
        if self.is_rule_context:
            self.rule_name = self.node_type.replace('RuleContext: ', '')
        elif self.is_terminal:
            self.token_name = self.node_type.replace('Terminal: ', '')
        else:
            self.rule_name = 'unknown'
    
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
    
    def accept(self, visitor):
        """Accept visitor pattern - delegates to visitor methods"""
        if hasattr(visitor, f'visit{self.rule_name}') and self.is_rule_context:
            method = getattr(visitor, f'visit{self.rule_name}')
            return method(self)
        elif hasattr(visitor, 'visitChildren'):
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
    
    # Properties that your extraction code might check
    @property 
    def start(self):
        """Mock start token"""
        return MockToken(self.start_index, self.text)
    
    @property
    def stop(self):
        """Mock stop token"""  
        return MockToken(self.stop_index, self.text)
    
    def getStart(self):
        """Get start token - ParserRuleContext method"""
        return self.start
    
    def getStop(self):
        """Get stop token - ParserRuleContext method"""
        return self.stop
    
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
                   if child.rule_name == base_name]
        
        # For single child node access
        for child in self._get_all_children():
            if child.rule_name == name or child.token_name == name:
                return child
        
        # If not found, return None (like ANTLR does)
        return None
    
    def _get_all_children(self):
        """Get all children as MockParseTree objects"""
        if self._children_cache is None:
            self._children_cache = []
            for i in range(self.getChildCount()):
                child = self.getChild(i)
                if child:
                    self._children_cache.append(child)
        return self._children_cache
    
    def __str__(self):
        return f"MockParseTree[{self.rule_name if self.is_rule_context else self.token_name}]: {self.text}"
    
    def __repr__(self):
        return self.__str__()


class MockToken:
    """Mock token object for compatibility with ANTLR Token interface"""
    
    def __init__(self, index: int, text: str):
        self.index = index
        self.text = text
        self.start_index = index
        self.stop_index = index + len(text) - 1 if text else index
    
    def getStartIndex(self):
        return self.start_index
    
    def getStopIndex(self):
        return self.stop_index
    
    def getText(self):
        return self.text


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
    
    return MockParseTree(json_data, parser_rule_names)


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
    return MockParseTree(json_data, parser_rule_names)


# Example usage demonstrating the exact replacement:
"""
# OLD CODE (using ANTLR directly):
# lexer = CommonLexerRules(InputStream(sql_text))
# stream = CommonTokenStream(lexer) 
# parser = TeraSql(stream)
# tree = parser.parse()  # <-- This is what you want to replace
# result = your_extraction_function(tree)

# NEW CODE (using JSON):
rule_names = ['parse', 'selectStatement', 'columnReference', ...]  # Get from Java
tree = load_tree_from_json('your_file.json', rule_names)  # <-- Direct replacement
result = your_extraction_function(tree)  # <-- Same function, unchanged!
"""
