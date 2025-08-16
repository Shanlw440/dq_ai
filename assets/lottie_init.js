// Renders /assets/data_downloading.json into #lottie-container using lottie-web
(function () {
  function mount() {
    if (!window.lottie) return setTimeout(mount, 120); // wait for lottie-web to load
    var el = document.getElementById("lottie-container");
    if (!el) return setTimeout(mount, 120);

    fetch("/assets/data_downloading.json")
      .then((r) => r.json())
      .then((data) => {
        window.lottie.loadAnimation({
          container: el,
          renderer: "svg",
          loop: true,
          autoplay: true,
          animationData: data,
        });
      })
      .catch((e) => console.error("Lottie load error:", e));
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mount);
  } else {
    mount();
  }
})();
