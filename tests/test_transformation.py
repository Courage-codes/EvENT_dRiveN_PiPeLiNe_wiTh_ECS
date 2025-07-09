import pytest
import pandas as pd
from decimal import Decimal

class TestTransformation:
    
    def test_data_loading(self):
        """Test basic data loading and structure"""
        # Mock data for testing
        orders_data = {
            'order_id': [1, 2, 3],
            'user_id': [101, 102, 103],
            'created_at': ['2025-01-01', '2025-01-02', '2025-01-03'],
            'status': ['completed', 'completed', 'returned']
        }
        
        items_data = {
            'id': [1, 2, 3],
            'order_id': [1, 2, 3],
            'product_id': [201, 202, 203],
            'sale_price': [10.50, 25.00, 15.75],
            'status': ['completed', 'completed', 'returned']
        }
        
        products_data = {
            'id': [201, 202, 203],
            'sku': ['SKU001', 'SKU002', 'SKU003'],
            'cost': [5.00, 12.00, 8.00],
            'category': ['Electronics', 'Clothing', 'Books'],
            'retail_price': [15.00, 30.00, 20.00]
        }
        
        orders_df = pd.DataFrame(orders_data)
        items_df = pd.DataFrame(items_data)
        products_df = pd.DataFrame(products_data)
        
        # Test data structure
        assert len(orders_df) == 3
        assert len(items_df) == 3
        assert len(products_df) == 3
        
        # Test required columns exist
        assert 'order_id' in orders_df.columns
        assert 'sale_price' in items_df.columns
        assert 'category' in products_df.columns
    
    def test_data_transformation(self):
        """Test data type conversions"""
        # Test date conversion
        orders_data = {'created_at': ['2025-01-01', '2025-01-02']}
        orders_df = pd.DataFrame(orders_data)
        orders_df['order_date'] = pd.to_datetime(orders_df['created_at']).dt.date
        
        assert orders_df['order_date'].dtype == 'object'
        
        # Test price conversion
        items_data = {'sale_price': ['10.50', '25.00']}
        items_df = pd.DataFrame(items_data)
        items_df['sale_price'] = items_df['sale_price'].astype(float)
        
        assert items_df['sale_price'].dtype == 'float64'
    
    def test_metrics_calculation(self):
        """Test basic metrics calculations"""
        # Sample data
        data = {
            'order_id': [1, 1, 2, 2, 3],
            'sale_price': [10.0, 15.0, 20.0, 25.0, 30.0],
            'status': ['completed', 'completed', 'completed', 'returned', 'completed'],
            'category': ['A', 'A', 'B', 'B', 'C']
        }
        df = pd.DataFrame(data)
        
        # Test total revenue
        total_revenue = df['sale_price'].sum()
        assert total_revenue == 100.0
        
        # Test revenue by category
        category_revenue = df.groupby('category')['sale_price'].sum()
        assert category_revenue['A'] == 25.0
        assert category_revenue['B'] == 45.0
        assert category_revenue['C'] == 30.0
        
        # Test return rate calculation
        df['is_returned'] = (df['status'] == 'returned').astype(int)
        return_rate = df['is_returned'].sum() / df['order_id'].nunique()
        assert return_rate == 1/3  # 1 returned order out of 3 unique orders
    
    def test_decimal_conversion(self):
        """Test Decimal conversion for DynamoDB"""
        test_values = [10.5, 25, 0.75, 100]
        
        for value in test_values:
            decimal_value = Decimal(str(value))
            assert isinstance(decimal_value, Decimal)
            assert float(decimal_value) == value
    
    def test_join_operations(self):
        """Test DataFrame join operations"""
        orders_df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'order_date': ['2025-01-01', '2025-01-02', '2025-01-03']
        })
        
        items_df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'product_id': [101, 102, 103],
            'sale_price': [10.0, 20.0, 30.0]
        })
        
        # Test join
        joined_df = items_df.merge(orders_df, on='order_id')
        
        assert len(joined_df) == 3
        assert 'order_date' in joined_df.columns
        assert 'sale_price' in joined_df.columns
