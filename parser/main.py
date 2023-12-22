import unicodedata
import pyparsing as pp
import sys
import re
from pprint import pprint
import json


def loose_question():
    question = pp.rest_of_line().set_results_name("question_body")

    def build_option(letter: str):
        return (
            pp.LineStart()
            + pp.CaselessLiteral(letter)
            + pp.ZeroOrMore(pp.one_of(") ."))
            + pp.ZeroOrMore(pp.White())
            + pp.rest_of_line().set_results_name(f"option_{letter}_answer")
        )

    return (
        question
        + build_option("A")
        + build_option("B")
        + build_option("C")
        + build_option("D")
    )


def structured_question():
    question_header = (
        pp.SkipTo("Question").set_results_name("question_header")
        + "Question"
        + pp.Optional(":")
        + pp.Optional(pp.LineEnd())
    )
    question_body = pp.SkipTo("Option A:").set_results_name("question_body")
    question = question_header + question_body

    def build_option(letter: str):
        explanation = (
            "Explanation:"
            + pp.Optional(
                pp.White()
                + pp.MatchFirst(
                    [
                        pp.Literal("Correct")
                        .set_parse_action(lambda: "True")
                        .set_results_name(f"option_{letter}_correct"),
                        pp.Literal("Incorrect")
                        .set_parse_action(lambda: "False")
                        .set_results_name(f"option_{letter}_correct"),
                    ]
                )
                + "."
            )
            + pp.SkipTo(pp.LineEnd())
            .set_results_name(f"option_{letter}_explanation")
            + pp.LineEnd()
        )

        return (
            f"Option {letter}:"
            + pp.Suppress(pp.LineEnd())
            + "Answer:"
            + pp.rest_of_line().set_results_name(f"option_{letter}_answer")
            + pp.Optional(explanation)
            + pp.Optional(
                "Correct:"
                + pp.ZeroOrMore(pp.White())
                + pp.rest_of_line().set_results_name(f"option_{letter}_correct")
            )
        )

    return (
        question
        + build_option("A")
        + build_option("B")
        + build_option("C")
        + build_option("D")
    )


def parse_explanation(explanation):
    if re.search(r"Correct.*?\..+", explanation[0]):
        return "True"
    if re.search(r"Incorrect.*?\..+", explanation[0]):
        return "False"
    return explanation


input0 = """Question:
As originally ratified, the United States Constitution provided for

Option A:
Answer:a presidential Cabinet
Explanation:
Correct: False

Option B:
Answer:a two-term presidential limit
Explanation:
Correct: False

Option C:
Answer:an electoral college
Explanation:
Correct: True

Option D:
Answer:political parties
Explanation:
Correct: False"""

input1 = """How did the Industrial Revolution affect the way products were made?
A) Goods were made by hand in small shops.
B) Factories used machines to make goods faster and cheaper.
C) People stopped buying things.
D) Products were only made at home.
"""

input2 = """Why did cities grow during industrialization?
A. Because cities had better schools.
B. Because there were more parks in the cities.
C. More job opportunities in factories attracted people.
D. People wanted to live farther from their work."""

input3 = """Why is Dorothea Dix a significant figure in U.S. history?
A She was a leader in the fight for the abolition of slavery.
B She led a reform effort to improve working conditions in factories.
C She was a leader in the fight for women’s suffrage.
D She led a reform effort to improve care for the mentally ill."""

input4 = """Question:
Cholesterol is an important component of animal cell membranes. Cholesterol molecules are often delivered to body cells by the blood, which transports the molecules in the form of cholesterol-protein complexes. The complexes must be moved into the body cells before the cholesterol molecules can be incorporated into the phospholipid bilayers of cell membranes.

Based on the information presented, which of the following is the most likely explanation for a buildup of cholesterol molecules in the blood of an animal?

Option A:
Answer:The animal’s body cells are defective in exocytosis.
Explanation:Incorrect. The cholesterol is delivered to the body cells by the blood and most likely taken up by endocytosis. If the animal’s body cells were defective in exocytosis, the cholesterol might build up inside the cells but not in the blood.

Option B:
Answer:The animal’s body cells are defective in endocytosis.
Explanation:Correct. The cholesterol-protein complexes are most likely moved into the body cells by endocytosis. A defect in endocytosis is likely to result in a buildup of cholesterol in the blood.

Option C:
Answer:The animal’s body cells are defective in cholesterol synthesis.
Explanation:Incorrect. Based on the information presented, the body cells take up cholesterol from the blood and do not synthesize cholesterol. A defect in the ability of the animal’s body cells to synthesize cholesterol would not explain a buildup of cholesterol in the blood.

Option D:
Answer:The animal’s body cells are defective in phospholipid synthesis.
Explanation:Incorrect. The phospholipid bilayers of the animal’s body cells are most likely intact and functioning properly. A defect in the ability of the animal’s body cells to synthesize phospholipids would not explain a buildup of cholesterol in the blood."""


if __name__ == "__main__":
    parser = pp.MatchFirst([structured_question(), loose_question()])

    res = parser.parse_string(sys.stdin.read())
    d = res.as_dict()
    # for key, value in res.as_dict().items():
    #     d[key] = unicodedata.normalize("NFKD", value)
    print(json.dumps(d))
    # pprint(res.as_dict())
