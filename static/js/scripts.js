function applySearchFilters({ zeroAlcOverride } = {}) {
  const priceMinEl = document.getElementById("priceMin");
  const priceMaxEl = document.getElementById("priceMax");
  const priceMin = priceMinEl ? priceMinEl.value : "";
  const priceMax = priceMaxEl ? priceMaxEl.value : "";
  const urlParams = new URLSearchParams(window.location.search);
  const q = urlParams.get("q") || "";
  const order = urlParams.get("order") || "score-asc";
  const storeEl = document.getElementById("storeSelect");
  const store = storeEl ? storeEl.value : "all";
  const scrapedAge = urlParams.get("scraped_age") || "";
  const currentZeroActive = urlParams.get("zero-alc") === "true";
  const zeroAlcActive =
    zeroAlcOverride === undefined ? currentZeroActive : zeroAlcOverride;

  let url =
    "/search?q=" +
    encodeURIComponent(q) +
    "&order=" +
    order +
    "&store=" +
    store;

  if (priceMin) {
    url += "&price_min=" + priceMin;
  }
  if (priceMax) {
    url += "&price_max=" + priceMax;
  }
  if (scrapedAge) {
    url += "&scraped_age=" + scrapedAge;
  }
  if (zeroAlcActive) {
    url += "&zero-alc=true";
  }

  window.location.href = url;
}

$(document).ready(function () {
  const coverEl = document.getElementById("cover");
  const slideEl = document.getElementById("slide");
  if (coverEl && slideEl) {
    coverEl.addEventListener("click", function () {
      slideEl.checked = false;
      coverEl.classList.toggle("shadow");
      coverEl.classList.toggle("clickable");
    });
  }

  const searchButton = document.getElementById("searchButton");
  if (searchButton) {
    searchButton.addEventListener("click", function () {
      const form =
        searchButton.closest("form") ||
        document.getElementById("query") ||
        document.getElementById("homepageSearch");
      if (form) {
        form.submit();
      }
    });
  }

  if (slideEl) {
    slideEl.checked = false;
  }

  const homeZeroCheckbox = document.getElementById("zeroAlcHomeCheckbox");
  const homeCategoryLinks = document.querySelectorAll(
    "#homepageCategoryButtons a[data-base-href]"
  );
  const homeZeroInput = document.getElementById("homeZeroAlcInput");

  function syncHomeZeroLinks() {
    if (!homeZeroCheckbox) {
      return;
    }
    const suffix = homeZeroCheckbox.checked ? "&zero-alc=true" : "";
    homeCategoryLinks.forEach((anchor) => {
      const baseHref =
        anchor.dataset.baseHref || anchor.getAttribute("href") || "";
      anchor.href = baseHref + suffix;
    });
    if (homeZeroInput) {
      homeZeroInput.value = homeZeroCheckbox.checked ? "true" : "";
    }
  }

  if (homeZeroCheckbox) {
    syncHomeZeroLinks();
    homeZeroCheckbox.addEventListener("change", syncHomeZeroLinks);
  }

  function triggerZeroAlcFilter() {
    const currentUrl = new URL(window.location.href);
    const params = currentUrl.searchParams;
    const isActive = params.get("zero-alc") === "true";
    if (isActive) {
      params.delete("zero-alc");
    } else {
      params.set("zero-alc", "true");
    }
    const queryString = params.toString();
    window.location.href = queryString
      ? `${currentUrl.pathname}?${queryString}`
      : currentUrl.pathname;
  }

  const zeroAlcCheckbox = document.getElementById("zeroAlcFilter");
  if (zeroAlcCheckbox) {
    zeroAlcCheckbox.addEventListener("change", triggerZeroAlcFilter);
  }

  const priceMin = document.getElementById("priceMin");
  if (priceMin) {
    priceMin.addEventListener("keypress", function (e) {
      if (e.key === "Enter") applySearchFilters();
    });
  }

  const priceMax = document.getElementById("priceMax");
  if (priceMax) {
    priceMax.addEventListener("keypress", function (e) {
      if (e.key === "Enter") applySearchFilters();
    });
  }
});
