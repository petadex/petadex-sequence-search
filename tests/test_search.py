"""
Local tests for Lambda function
Run with: python -m pytest tests/
"""

import json
import sys
sys.path.insert(0, '.')

from lambda_function import validate_sequence, handler

def test_validate_sequence():
    """Test sequence validation"""
    
    # Valid sequence
    seq = validate_sequence("MKLLIVLLALAVAALHA")
    assert seq == "MKLLIVLLALAVAALHA"
    
    # With whitespace
    seq = validate_sequence("MKL LIV LLA")
    assert seq == "MKLLIVLLA"
    
    # Too short
    try:
        validate_sequence("MKL")
        assert False, "Should raise error"
    except ValueError:
        pass

def test_handler_validation():
    """Test handler with invalid input"""
    
    event = {
        'sequence': 'INVALID123'
    }
    
    response = handler(event, None)
    assert response['statusCode'] == 400

# Add more tests as needed
