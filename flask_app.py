from flask import Flask

app = Flask(__name__)

@app.route("/reddit_political_sentiment_analysis")
def reddit_political_sentiment():
    html = '''
        <script>
            function mainJSFunction() {
                // Get the screen width and height
                const screenWidth = window.screen.width;
                const screenHeight = window.screen.height;

                // Get iframe ID
                var iframe = document.getElementById('powerBIiframeID')

                // Set width and height
                var width = screenWidth;
                var height = (373.5/600) * screenWidth;

                // Reset width and height if needed
                if (height > screenHeight - 285) {
                    var height = screenHeight - 285
                    var width = (600/373.5) * height
                }

                // Set iframe width and height
                iframe.width = width;
                iframe.height = height;
            }
        </script>
        <body onload="mainJSFunction()">
            <div style="text-align:center;">
                <h1>Political Candidates' Reddit Sentiment Dashboard</h1>
                <p>Dashboard updates every 3 hours with the latest Reddit data</p>
                <br>
                <iframe title="candidate_reddit_political_sentiment_dashboard" id="powerBIiframeID" src="src" frameborder="0" allowFullScreen="true"></iframe>
                <br>
                <h1><a href="https://github.com/ghappy112/reddit_political_sentiment">GitHub</a></h1>
            </div>
        </body>
        '''
    return html

if "__name__" == "__main__":
    app.run(debug=True)
