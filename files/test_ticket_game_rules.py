import app


def test_mega_millions_accepts_mega_ball_25():
    assert app.TICKET_GAME_RULES["MM"]["bonus_max"] == 25
    assert app.validate_ticket_numbers("MM", [1, 2, 3, 4, 5, 25]) == (True, "")


def test_mega_millions_rejects_mega_ball_above_25():
    valid, message = app.validate_ticket_numbers("MM", [1, 2, 3, 4, 5, 26])

    assert valid is False
    assert message == "The 6th number must be between 1 and 25."
