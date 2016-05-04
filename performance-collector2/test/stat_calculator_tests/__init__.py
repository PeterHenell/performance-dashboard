import unittest

from stat_calculator import StatCalculator


class StatCalculatorTestCase(unittest.TestCase):

    def test_calculate_stats(self):
        row = {'k': 1, 'v': 1000}
        cached_row = {'k': 1, 'v': 700}

        calculated = StatCalculator.calculate_row_delta(row=row,
                                                        cached_row=cached_row,
                                                        data_key_col='k',
                                                        timestamp=1)
        self.assertEquals(calculated.key_column_name, 'k')
        self.assertEquals(calculated.timestamp, 1)
        self.assertEquals(len(calculated.delta_fields), 2)
        self.assertEquals(calculated.delta_fields['k'].measured, 1, "delta of key should be the value of the key")
        self.assertEquals(calculated.delta_fields['k'].delta, None, "delta of key should be None")
        self.assertEquals(calculated.delta_fields['k'].previous, None)

        self.assertEquals(calculated.delta_fields['v'].measured, 1000)
        self.assertEquals(calculated.delta_fields['v'].delta, 300)
        self.assertEquals(calculated.delta_fields['v'].previous, 700)

    def test_calculate_stats_with_empy_cached_row(self):
        row = {'k': 1, 'v': 1000}
        cached_row = {}

        calculated = StatCalculator.calculate_row_delta(row=row,
                                                        cached_row=cached_row,
                                                        data_key_col='k',
                                                        timestamp=1)

        self.assertEquals(calculated.delta_fields['v'].measured, 1000)
        self.assertEquals(calculated.delta_fields['v'].delta, None)
        self.assertEquals(calculated.delta_fields['v'].previous, None)

    def test_calculate_stats_for_several_fields(self):
        row = {'k': 1, 'v': 1000, 'v2': 40.6}
        cached_row = {'k': 1, 'v': 300, 'v2': 10.3}

        calculated = StatCalculator.calculate_row_delta(row=row,
                                                        cached_row=cached_row,
                                                        data_key_col='k',
                                                        timestamp=1)

        self.assertEquals(calculated.delta_fields['v'].measured, 1000)
        self.assertEquals(calculated.delta_fields['v'].delta, 700)
        self.assertEquals(calculated.delta_fields['v'].previous, 300)

        self.assertEquals(calculated.delta_fields['v2'].measured, 40.6)
        self.assertEquals(calculated.delta_fields['v2'].delta, 30.3)
        self.assertEquals(calculated.delta_fields['v2'].previous, 10.3)

    def test_calculate_stats_value_which_was_reset(self):
        # new row is much less than old row values
        row = {'k': 1, 'v': 200, 'v2': 1.6}
        cached_row = {'k': 1, 'v': 300, 'v2': 10.3}

        calculated = StatCalculator.calculate_row_delta(row=row,
                                                        cached_row=cached_row,
                                                        data_key_col='k',
                                                        timestamp=1)

        self.assertEquals(calculated.delta_fields['v'].measured, 200)
        self.assertEquals(calculated.delta_fields['v'].delta, None)
        self.assertEquals(calculated.delta_fields['v'].previous, None)

        self.assertEquals(calculated.delta_fields['v2'].measured, 1.6)
        self.assertEquals(calculated.delta_fields['v2'].delta, None)
        self.assertEquals(calculated.delta_fields['v2'].previous, None)

if __name__ == '__main__':
    unittest.main()