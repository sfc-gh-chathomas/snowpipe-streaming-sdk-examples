import os
from pathlib import Path
import unittest
from unittest.mock import patch

from streamlit.testing.v1 import AppTest


class RenderTests(unittest.TestCase):
    def test_offline_render_and_message_opt_in(self):
        app_path = Path(__file__).resolve().parents[1] / 'streamlit_app.py'
        with patch.dict(os.environ, {'DASHBOARD_PREVIEW': '1'}):
            app = AppTest.from_file(str(app_path)).run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            self.assertEqual(len(app.metric), 4)
            self.assertTrue(any('Synthetic preview' in warning.value for warning in app.warning))
            app.checkbox[0].check()
            app.button[0].click()
            app.run(timeout=30)
            self.assertEqual(len(app.exception), 0)
            self.assertTrue(any('sensitive data' in warning.value for warning in app.warning))


if __name__ == '__main__':
    unittest.main()
