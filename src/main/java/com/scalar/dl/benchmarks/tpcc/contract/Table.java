package com.scalar.dl.benchmarks.tpcc.contract;

public final class Table {
  public static final String KEY_TABLE_NAME = "table_name";

  private Table() {}

  // Use public static final int since enum cannot be used in contract.
  public static class Code {
    public static final int CUSTOMER = 0;
    public static final int CUSTOMER_SECONDARY = 1;
    public static final int DISTRICT = 2;
    public static final int HISTORY = 3;
    public static final int ITEM = 4;
    public static final int NEW_ORDER = 5;
    public static final int ORDER = 6;
    public static final int ORDER_SECONDARY = 7;
    public static final int ORDER_LINE = 8;
    public static final int STOCK = 9;
    public static final int WAREHOUSE = 10;
  }

  public static class Name {
    public static final String CUSTOMER = "customer";
    public static final String CUSTOMER_SECONDARY = "customer_secondary";
    public static final String DISTRICT = "district";
    public static final String HISTORY = "history";
    public static final String ITEM = "item";
    public static final String NEW_ORDER = "new_order";
    public static final String ORDER = "order";
    public static final String ORDER_SECONDARY = "order_secondary";
    public static final String ORDER_LINE = "order_line";
    public static final String STOCK = "stock";
    public static final String WAREHOUSE = "warehouse";
  }

  public static class QueryParam {
    public static final String KEY_ORDER_LINE_COUNT = "ol_count";
    public static final String KEY_REMOTE = "remote";
    public static final String KEY_ITEMS = "items";
    public static final String KEY_BY_LAST_NAME = "by_last_name";
    public static final String KEY_DATE = "date";
    public static final String KEY_NONCE = "nonce";
  }

  public static class Customer {
    public static final String NAME = "customer";
    public static final String PREFIX = "c_";
    public static final String KEY_WAREHOUSE_ID = "c_w_id";
    public static final String KEY_DISTRICT_ID = "c_d_id";
    public static final String KEY_CUSTOMER_ID = "c_id";
    public static final String KEY_FIRST = "c_first";
    public static final String KEY_MIDDLE = "c_middle";
    public static final String KEY_LAST = "c_last";
    public static final String KEY_DISCOUNT = "c_discount";
    public static final String KEY_CREDIT = "c_credit";
    public static final String KEY_CREDIT_LIM = "c_credit_lim";
    public static final String KEY_BALANCE = "c_balance";
    public static final String KEY_YTD_PAYMENT = "c_ytd_payment";
    public static final String KEY_PAYMENT_CNT = "c_payment_cnt";
    public static final String KEY_DELIVERY_CNT = "c_delivery_cnt";
    public static final String KEY_ADDRESS = "c_address";
    public static final String KEY_STREET_1 = "c_street_1";
    public static final String KEY_STREET_2 = "c_street_2";
    public static final String KEY_CITY = "c_city";
    public static final String KEY_STATE = "c_state";
    public static final String KEY_ZIP = "c_zip";
    public static final String KEY_PHONE = "c_phone";
    public static final String KEY_SINCE = "c_since";
    public static final String KEY_DATA = "c_data";
    public static final String KEY_INDEX = "c_index";

    public static final int UNUSED_ID = 0;
    public static final int MIN_FIRST = 8;
    public static final int MAX_FIRST = 16;
    public static final int MIN_DATA = 300;
    public static final int MAX_DATA = 500;
    public static final int PHONE_SIZE = 16;
  }

  public static class CustomerSecondary {
    public static final String NAME = "customer_secondary";
    public static final String KEY_WAREHOUSE_ID = "c_w_id";
    public static final String KEY_DISTRICT_ID = "c_d_id";
    public static final String KEY_LAST = "c_last";
    public static final String KEY_CUSTOMER_ID_LIST = "c_id_list";
    public static final String KEY_CUSTOMER_ID = "c_id";
    public static final String KEY_FIRST = "c_first";
  }

  public static class District {
    public static final String NAME = "district";
    public static final String PREFIX = "d_";
    public static final String KEY_WAREHOUSE_ID = "d_w_id";
    public static final String KEY_DISTRICT_ID = "d_id";
    public static final String KEY_NAME = "d_name";
    public static final String KEY_YTD = "d_ytd";
    public static final String KEY_TAX = "d_tax";
    public static final String KEY_NEXT_O_ID = "d_next_o_id";

    public static final int CUSTOMERS = 3000;
    public static final int ORDERS = 3000;
    public static final int MIN_NAME = 6;
    public static final int MAX_NAME = 10;
  }

  public static class History {
    public static final String NAME = "history";
    public static final String KEY_CUSTOMER_ID = "h_c_id";
    public static final String KEY_CUSTOMER_DID = "h_c_d_id";
    public static final String KEY_CUSTOMER_WID = "h_c_w_id";
    public static final String KEY_DISTRICT_ID = "h_d_id";
    public static final String KEY_WAREHOUSE_ID = "h_w_id";
    public static final String KEY_DATE = "h_date";
    public static final String KEY_AMOUNT = "h_amount";
    public static final String KEY_DATA = "h_data";

    public static final int MIN_DATA = 12;
    public static final int MAX_DATA = 24;
  }

  public static class Item {
    public static final String NAME = "item";
    public static final String KEY_ITEM_ID = "i_id";
    public static final String KEY_NAME = "i_name";
    public static final String KEY_PRICE = "i_price";
    public static final String KEY_DATA = "i_data";
    public static final String KEY_IM_ID = "i_im_id";

    public static final int ITEMS = 100000;
    public static final int MIN_NAME = 14;
    public static final int MAX_NAME = 24;
    public static final int MIN_DATA = 26;
    public static final int MAX_DATA = 50;
    public static final int UNUSED_ID = 1;
  }

  public static class NewOrder {
    public static final String NAME = "new_order";
    public static final String KEY_WAREHOUSE_ID = "no_w_id";
    public static final String KEY_DISTRICT_ID = "no_d_id";
    public static final String KEY_ORDER_ID = "no_o_id";
  }

  public static class Order {
    public static final String NAME = "order";
    public static final String KEY_WAREHOUSE_ID = "o_w_id";
    public static final String KEY_DISTRICT_ID = "o_d_id";
    public static final String KEY_ORDER_ID = "o_id";
    public static final String KEY_CUSTOMER_ID = "o_c_id";
    public static final String KEY_CARRIER_ID = "o_carrier_id";
    public static final String KEY_OL_CNT = "o_ol_cnt";
    public static final String KEY_ALL_LOCAL = "o_all_local";
    public static final String KEY_ENTRY_D = "o_entry_d";
  }

  public static class OrderSecondary {
    public static final String NAME = "order_secondary";
  }

  public static class OrderLine {
    public static final String NAME = "order_line";
    public static final String KEY_WAREHOUSE_ID = "ol_w_id";
    public static final String KEY_DISTRICT_ID = "ol_d_id";
    public static final String KEY_ORDER_ID = "ol_o_id";
    public static final String KEY_NUMBER = "ol_number";
    public static final String KEY_ITEM_ID = "ol_i_id";
    public static final String KEY_DELIVERY_D = "ol_delivery_d";
    public static final String KEY_AMOUNT = "ol_amount";
    public static final String KEY_SUPPLY_W_ID = "ol_supply_w_id";
    public static final String KEY_QUANTITY = "ol_quantity";
    public static final String KEY_DIST_INFO = "ol_dist_info";

    public static final int MIN_PER_ORDER = 5;
    public static final int MAX_PER_ORDER = 15;
    public static final int DIST_INFO_SIZE = 24;
  }

  public static class Stock {
    public static final String NAME = "stock";
    public static final String KEY_WAREHOUSE_ID = "s_w_id";
    public static final String KEY_ITEM_ID = "s_i_id";
    public static final String KEY_QUANTITY = "s_quantity";
    public static final String KEY_YTD = "s_ytd";
    public static final String KEY_ORDER_CNT = "s_order_cnt";
    public static final String KEY_REMOTE_CNT = "s_remote_cnt";
    public static final String KEY_DIST_PREFIX = "s_dist_";
    public static final String KEY_DISTRICT01 = "s_dist_01";
    public static final String KEY_DISTRICT02 = "s_dist_02";
    public static final String KEY_DISTRICT03 = "s_dist_03";
    public static final String KEY_DISTRICT04 = "s_dist_04";
    public static final String KEY_DISTRICT05 = "s_dist_05";
    public static final String KEY_DISTRICT06 = "s_dist_06";
    public static final String KEY_DISTRICT07 = "s_dist_07";
    public static final String KEY_DISTRICT08 = "s_dist_08";
    public static final String KEY_DISTRICT09 = "s_dist_09";
    public static final String KEY_DISTRICT10 = "s_dist_10";

    public static final int MIN_DATA = 26;
    public static final int MAX_DATA = 50;
    public static final int DIST_SIZE = 24;
  }

  public static class Warehouse {
    public static final String NAME = "warehouse";
    public static final String PREFIX = "w_";
    public static final String KEY_WAREHOUSE_ID = "w_id";
    public static final String KEY_WAREHOUSE_NAME = "w_name";
    public static final String KEY_YTD = "w_ytd";
    public static final String KEY_TAX = "w_tax";

    public static final int DISTRICTS = 10;
    public static final int ITEMS = 100000;
    public static final int MIN_NAME = 6;
    public static final int MAX_NAME = 10;
  }

  public static class Address {
    public static final String KEY_STREET_1 = "street_1";
    public static final String KEY_STREET_2 = "street_2";
    public static final String KEY_CITY = "city";
    public static final String KEY_STATE = "state";
    public static final String KEY_ZIP = "zip";

    public static final int MIN_STREET = 10;
    public static final int MAX_STREET = 20;
    public static final int MIN_CITY = 10;
    public static final int MAX_CITY = 20;
    public static final int STATE_SIZE = 2;
    public static final int ZIP_SIZE = 4;
  }
}
