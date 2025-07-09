import pytest

class TestValidation:
    
    def test_orders_schema_valid(self):
        """Test valid orders schema"""
        required_fields = ['order_id', 'user_id', 'created_at', 'status']
        test_headers = ['order_id', 'user_id', 'created_at', 'status', 'extra_field']
        
        missing_fields = [field for field in required_fields if field not in test_headers]
        assert len(missing_fields) == 0, f"Missing required fields: {missing_fields}"
    
    def test_orders_schema_invalid(self):
        """Test invalid orders schema"""
        required_fields = ['order_id', 'user_id', 'created_at', 'status']
        test_headers = ['order_id', 'user_id', 'created_at']  # missing 'status'
        
        missing_fields = [field for field in required_fields if field not in test_headers]
        assert len(missing_fields) > 0, "Should have missing fields"
    
    def test_order_items_schema_valid(self):
        """Test valid order_items schema"""
        required_fields = ['id', 'order_id', 'product_id', 'sale_price']
        test_headers = ['id', 'order_id', 'product_id', 'sale_price']
        
        missing_fields = [field for field in required_fields if field not in test_headers]
        assert len(missing_fields) == 0
    
    def test_products_schema_valid(self):
        """Test valid products schema"""
        required_fields = ['id', 'sku', 'cost', 'category', 'retail_price']
        test_headers = ['id', 'sku', 'cost', 'category', 'retail_price']
        
        missing_fields = [field for field in required_fields if field not in test_headers]
        assert len(missing_fields) == 0
    
    def test_csv_file_detection(self):
        """Test CSV file detection logic"""
        test_files = [
            'orders.csv',
            'data.CSV',
            'test.txt',
            'folder/',
            'orders_part1.csv'
        ]
        
        csv_files = [f for f in test_files if f.lower().endswith('.csv') and not f.endswith('/')]
        assert len(csv_files) == 3
        assert 'orders.csv' in csv_files
        assert 'data.CSV' in csv_files
        assert 'orders_part1.csv' in csv_files
