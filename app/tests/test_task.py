from app.models.task import Task



class GeopriceTaskTestCase(unittest.TestCase):
    """ Test Case for Geoprice Consumer
    """

    @classmethod
    def setUpClass(cls):
        """ Initializes the database
        """
        # Define test database
        print("Setting up tests")
        return

    @classmethod
    def tearDownClass(cls):
        """ Drops database
        """
        print("Teardown class")
        return
        if config.TESTING:
            with app.app.app_context():
                app.dropdb_cmd()

    def setUp(self):
        """ Set up
        """
        # Init Flask ctx
        self.ctx = app.app.app_context()
        self.ctx.push()
        app.get_redis()

    def tearDown(self):
        # Dropping flask ctx
        self.ctx.pop()

    def test_create_new_task(self):
        """ Testing DB prices i
        """ 
        global new_price
        print("Testing price validation")
        validate = Price.validate(new_price)
        self.assertTrue(validate)

    def test_price_validation_fail(self):
        global new_price
        print("Testing price validation failure!")

    def test_price_save_success(self):
        print("Validating save price success")
        global new_price
        pr = Price(new_price)
        result = pr.save_all()
        self.assertEqual(result, True)