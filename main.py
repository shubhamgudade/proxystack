import React, { useEffect, useRef, useState, useCallback } from 'react';
import './AppPage.css';
import '../stars/stars.css';
import LoadingOverlay from './LoadingOverlay.jsx';
import FodLoader from './FodLoader.jsx';
import {
  runFodHunt,
  fetchAccounts,
  deleteAccount,
  sendOtp,
  verifyOtp,
  fetchAddresses,
  resolveLocation,
  createAddress,
  searchProducts,
  fetchAnonForYou,
  fetchProductStatic,
  fetchProductDynamic,
  fetchCartMinview,
  fetchCartDetails,
  addToCart,
  removeFromCart,
  setCartLocation,
  bindAddressToCart,
  processCheckout,
  fetchOrders,
  fetchOrderDetails,
  fetchCancellationReasons,
  cancelOrder,
  parseSearchWidgets,
  parseProductStatic,
  parseProductDynamic,
  loadOrderSnapshot,
  saveOrderSnapshot,
  diffOrderSnapshot,
  genUUID,
  genHex,
  extractProductId,
  fetchDashboardStats,
  fetchRecentUpdates,
  triggerStatsRefresh,
  fetchRefreshStatus,
  fetchAddressCache,
  clearAddressCache,
  triggerAddressFetch,
  fetchAddressFetchStatus,
} from './lib/api';

const ORDERS_CACHE_KEY = 'mesoweb_orders_cache_v1';

function readOrdersCache() {
  try {
    const raw = localStorage.getItem(ORDERS_CACHE_KEY);
    return raw ? JSON.parse(raw) : { updatedAt: 0, accounts: {} };
  } catch {
    return { updatedAt: 0, accounts: {} };
  }
}

function writeOrdersCache(cache) {
  try { localStorage.setItem(ORDERS_CACHE_KEY, JSON.stringify(cache)); } catch {}
}

function orderCacheKey(order) {
  return String(order?.order_num ?? order?.sub_order_num ?? order?.id ?? '');
}

function mergeCachedOrders(cache, accountId, phone, orders) {
  const account = cache.accounts[accountId] ?? { phone, orders: [], details: {} };
  const previousDetails = account.details ?? {};
  const existingOrders = account.orders ?? [];
  const merged = new Map();

  existingOrders.forEach(order => {
    const key = orderCacheKey(order);
    if (key) merged.set(key, order);
  });

  orders.forEach(order => {
    const normalized = { ...order, phone: phone ?? order.phone ?? '', accountId };
    const key = orderCacheKey(normalized);
    if (key) merged.set(key, { ...merged.get(key), ...normalized });
  });

  account.phone = phone ?? account.phone ?? '';
  account.orders = Array.from(merged.values()).sort(
    (a, b) => (b.created_date ?? 0) - (a.created_date ?? 0)
  );
  account.details = previousDetails;
  cache.accounts[accountId] = account;
  cache.updatedAt = Date.now();
  return cache;
}

function getCachedOrdersForAccounts(cache, accounts, accountId) {
  const selected = accountId === 'all' ? accounts : accounts.filter(a => a.account_id === accountId);
  return selected.flatMap(acc => (cache.accounts[acc.account_id]?.orders ?? []).map(order => ({
    ...order, phone: acc.phone, accountId: acc.account_id,
  }))).sort((a, b) => (b.created_date ?? 0) - (a.created_date ?? 0));
}

// ─── Nav ─────────────────────────────────────────────────────────────────────

const navItems = [
  { id: 'home',    label: 'Home'    },
  { id: 'search',  label: 'Search'  },
  { id: 'cart',    label: 'Cart'    },
  { id: 'orders',  label: 'Orders'  },
  { id: 'profile', label: 'Profile' },
];

// ─── FlipCard ────────────────────────────────────────────────────────────────

function FlipCard({ card, pageReady, onOpen, delay }) {
  const [flipped, setFlipped] = useState(false);
  const [interacted, setInteracted] = useState(false);
  const tapTimer = useRef(null);

  const handleTap = () => {
    setInteracted(true);
    if (tapTimer.current) {
      window.clearTimeout(tapTimer.current);
      tapTimer.current = null;
      onOpen(card.id);
      return;
    }
    tapTimer.current = window.setTimeout(() => {
      tapTimer.current = null;
      setFlipped((c) => !c);
    }, 240);
  };

  return (
    <article
      className={`meso-card meso-card--${card.id}${pageReady ? ' is-ready' : ''}${interacted ? ' is-interacted' : ''}`}
      style={{ '--card-delay': `${delay}s` }}
      onClick={handleTap}
      onDoubleClick={(e) => e.preventDefault()}
      role="button" tabIndex={0}
      aria-label={`${card.label}. Tap to flip, double tap to open`}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); handleTap(); } }}
    >
      <div className={`meso-card__content${flipped ? ' is-flipped' : ''}`}>
        <div className="meso-card__face meso-card__back">
          <div className="meso-card__back-glow" aria-hidden="true" />
          <div className="meso-card__back-content">
            <div className={`meso-card__icon meso-card__icon--${card.id}`} aria-hidden="true">{card.icon}</div>
            <strong>{card.label}</strong>
            <span>{card.backText || 'Tap to view'}</span>
          </div>
        </div>
        <div className="meso-card__face meso-card__front">
          <div className="meso-card__orb" aria-hidden="true" />
          <div className="meso-card__front-content">
            <small>{card.label}</small>
            <div>
              <strong>{card.value}</strong>
              <p>{card.detail}</p>
            </div>
            <span>Double tap to open</span>
          </div>
        </div>
      </div>
    </article>
  );
}

// ─── AccountPicker ───────────────────────────────────────────────────────────

function AccountPicker({ value, onChange, accounts, label = 'Account', includeAnonymous = false, includeAll = false }) {
  const [open, setOpen] = useState(false);
  const options = [
    ...(includeAll ? [{ account_id: 'all', phone: 'All Accounts' }] : []),
    ...(includeAnonymous ? [{ account_id: 'anonymous', phone: 'Anonymous' }] : []),
    ...accounts,
  ];
  const selected = options.find((account) => account.account_id === value) || options[0];
  const selectedValue = selected?.account_id ?? '';

  return (
    <div className="fyp-account-dropdown meso-account-picker">
      <span className="meso-account-picker__label">{label}</span>
      <button
        type="button"
        className="fyp-account-dropdown__trigger"
        onClick={() => setOpen((current) => !current)}
        aria-expanded={open}
        aria-haspopup="listbox"
      >
        <span className="fyp-account-dropdown__current">
          <span className="fyp-account-dropdown__avatar">
            {selectedValue === 'anonymous' ? 'A' : selectedValue === 'all' ? 'ALL' : selected?.phone?.slice(-2) ?? '?'}
          </span>
          <span>
            <small>{label}</small>
            <strong>{selected?.phone ?? 'Select Account'}</strong>
          </span>
        </span>
        <svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="m7 9 5 5 5-5"/></svg>
      </button>
      {open && (
        <div className="fyp-account-dropdown__menu is-open" role="listbox">
          {options.map((account) => (
            <button
              type="button"
              key={account.account_id}
              className={`fyp-account-dropdown__option${selectedValue === account.account_id ? ' is-selected' : ''}`}
              onClick={() => { onChange(account.account_id); setOpen(false); }}
              role="option"
              aria-selected={selectedValue === account.account_id}
            >
              <span className="fyp-account-dropdown__avatar">
                {account.account_id === 'anonymous' ? 'A' : account.account_id === 'all' ? 'ALL' : account.phone?.slice(-2)}
              </span>
              <span><strong>{account.phone}</strong></span>
              <i>{selectedValue === account.account_id ? '✓' : ''}</i>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

function CartAccountPicker({ value, onChange, accounts }) {
  const [open, setOpen] = useState(false);
  const selected = accounts.find(a => a.account_id === value);

  return (
    <div className="cart-account-picker">
      <span className="cart-account-picker__label">Account</span>
      <button
        type="button"
        className="cart-account-picker__trigger"
        onClick={() => setOpen(current => !current)}
        aria-expanded={open}
        aria-haspopup="listbox"
      >
        <span className="cart-account-picker__current">
          <span className="cart-account-picker__avatar">{selected?.phone?.slice(-2) ?? '?'}</span>
          <span>
            <small>Account</small>
            <strong>{selected?.phone ?? 'Select account'}</strong>
          </span>
        </span>
        <svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="m7 9 5 5 5-5"/></svg>
      </button>

      {open && (
        <div className="cart-account-picker__menu" role="listbox">
          <button
            type="button"
            className={`cart-account-picker__option${!value ? ' is-selected' : ''}`}
            onClick={() => { onChange(''); setOpen(false); }}
            role="option"
            aria-selected={!value}
          >
            <span className="cart-account-picker__avatar">?</span>
            <strong>Select account</strong>
            <i>{!value ? '✓' : ''}</i>
          </button>

          {accounts.map(account => (
            <button
              type="button"
              key={account.account_id}
              className={`cart-account-picker__option${value === account.account_id ? ' is-selected' : ''}`}
              onClick={() => { onChange(account.account_id); setOpen(false); }}
              role="option"
              aria-selected={value === account.account_id}
            >
              <span className="cart-account-picker__avatar">{account.phone?.slice(-2)}</span>
              <strong>{account.phone}</strong>
              <i>{value === account.account_id ? '✓' : ''}</i>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── ProductCard ─────────────────────────────────────────────────────────────

function ProductCard({ product, pageReady, delay, onOpenDetail }) {
  const [threeDReady, setThreeDReady] = useState(false);
  const [flipped, setFlipped]         = useState(false);
  const tapTimer                      = useRef(null);

  const handleOpen = () => {
    if (!threeDReady) { setThreeDReady(true); requestAnimationFrame(() => setFlipped(true)); return; }
    setFlipped((c) => !c);
  };

  const handleCardClick = () => {
    if (tapTimer.current) {
      window.clearTimeout(tapTimer.current);
      tapTimer.current = null;
      onOpenDetail?.(product);
      return;
    }
    tapTimer.current = window.setTimeout(() => {
      tapTimer.current = null;
      handleOpen();
    }, 240);
  };

  useEffect(() => () => { if (tapTimer.current) window.clearTimeout(tapTimer.current); }, []);

  return (
    <article
      className={`product-card${pageReady ? ' is-ready' : ''}${threeDReady ? ' is-3d' : ''}${flipped ? ' is-flipped' : ''}`}
      style={{ '--product-delay': `${delay}s` }}
      onClick={handleCardClick}
      onDoubleClick={(e) => { e.preventDefault(); if (tapTimer.current) { window.clearTimeout(tapTimer.current); tapTimer.current = null; } onOpenDetail?.(product); }}
      role="button" tabIndex={0}
      aria-label={`${product.name}. Single tap to flip, double tap to open product details`}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpenDetail?.(product); } }}
    >
      <div className="product-card__content">
        <div className="product-card__face product-card__front">
          <img src={product.image} alt={product.name} loading="lazy" />
          <div className="product-card__image-shade" aria-hidden="true" />
          <div className="product-card__front-content">
            <div className="product-card__top-badges">
              {product.nextDayDispatch
                ? <span className="product-badge product-badge--ship"><svg className="product-badge__icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M13.2 2 5 13h6l-.8 9L19 10h-6z"/></svg><span>Next Day</span></span>
                : null}
              {(product.trustMarkers ?? []).map(t => (
                <span key={t} className="product-badge product-badge--mall">{t}</span>
              ))}
            </div>
            <div className="product-card__info">
              <div className="product-card__name-row">
                <strong>{product.name}</strong>
                <span className="product-card__save" aria-label="Save product"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20.8 8.7c0 5.2-8.8 10-8.8 10s-8.8-4.8-8.8-10A4.7 4.7 0 0 1 12 6.2a4.7 4.7 0 0 1 8.8 2.5Z"/></svg></span>
              </div>
              <div className="product-card__meta">
                <strong>{product.price}</strong>
                <span className="product-card__rating"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1.1 6.2-5.6-2.9-5.6 2.9 1.1-6.2L3 9.6l6.2-.9z"/></svg>{product.rating} <em>({product.reviews})</em></span>
              </div>
            </div>
          </div>
        </div>
        {threeDReady && (
          <div className="product-card__face product-card__back">
            <div className="product-card__back-top">
              <span>{product.name}</span>
              <small>Pricing & Delivery</small>
            </div>
            <div className="product-card__prices">
              <div><span>COD PRICE</span><strong>{product.codPrice}</strong></div>
              <div><span>UPI PRICE</span><strong>{product.upiPrice}</strong></div>
            </div>
            <div className="product-card__badges">
              {product.nextDayDispatch
                ? <span className="product-badge product-badge--ship product-badge--next-ship"><svg className="product-badge__icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M13.2 2 5 13h6l-.8 9L19 10h-6z"/></svg><span>NXT SHIP</span></span>
                : null}
              {product.seller && product.seller !== 'Meesho Seller'
                ? <span className="product-badge product-badge--mall">{product.seller}</span>
                : null}
            </div>
            <small className="product-card__flip-hint">Single tap to return · Double tap for details</small>
          </div>
        )}
      </div>
    </article>
  );
}

function LazyProductCard({ product, pageReady, delay, onOpenDetail }) {
  const slotRef       = useRef(null);
  const [visible, setVisible] = useState(false);
  useEffect(() => {
    const node = slotRef.current;
    if (!node || typeof IntersectionObserver === 'undefined') { setVisible(true); return; }
    const obs = new IntersectionObserver(([e]) => setVisible(e.isIntersecting), { threshold: 0.01 });
    obs.observe(node);
    return () => obs.disconnect();
  }, []);
  return (
    <div ref={slotRef} className="product-card-slot">
      {visible ? <ProductCard product={product} pageReady={pageReady} delay={delay} onOpenDetail={onOpenDetail} /> : null}
    </div>
  );
}

function ProductGrid({ pageReady, products = [], onOpenDetail }) {
  if (!products.length) return <div className="product-grid-empty">No products found.</div>;
  return (
    <div className="product-grid" aria-label="Products">
      {products.map((p, i) => (
        <LazyProductCard key={p.id ?? i} product={p} pageReady={pageReady} delay={i * 0.08} onOpenDetail={onOpenDetail} />
      ))}
    </div>
  );
}

// ─── Product Detail Page ─────────────────────────────────────────────────────

function ProductDetailPage({ product, onClose, onAddToCart, onBuyNow }) {
  const [selectedImage, setSelectedImage] = useState(0);
  const [quantity, setQuantity] = useState(1);
  const [adding,   setAdding]  = useState(false);
  const [saved,    setSaved]   = useState(false);
  const [selectedVariation, setSelectedVariation] = useState(null);

  const sizeOptions = (product.sizes ?? []).map(s => ({
    id: s.id, name: s.name ?? String(s), price: s.price, inStock: s.inStock !== false,
  }));
  const effectiveVariation = selectedVariation ?? sizeOptions.find(s => s.inStock) ?? null;

  const gallery = product.gallery?.length ? product.gallery : [product.image];
  const priceNumber = Number((product.codPrice ?? product.price ?? '').replace(/[^0-9]/g, '')) || 0;
  const upiNumber   = Number((product.upiPrice || '').replace(/[^0-9]/g, '')) || Math.max(priceNumber - 50, 0);
  const mrpNumber   = Number((product.mrpPrice || '').replace(/[^0-9]/g, '')) || Math.ceil((priceNumber * 1.25) / 10) * 10;
  const discountPct = mrpNumber ? Math.round(((mrpNumber - priceNumber) / mrpNumber) * 100) : 0;

  const attributes = product.attributes ?? [];
  const ndd = !!product.nextDayDispatch;

  const specs = [
    ['Product ID',  `#${String(product.productId || product.id || '').padStart(8, '0')}`],
    ['Catalog ID',  `#${String(product.catalogId || product.id || '').padStart(8, '0')}`],
    ['Supplier',    product.seller || 'Meesho Seller'],
    ['Type',        product.category || '—'],
    ['Country',     'India'],
  ];

  const runCartAnimation = (action) => {
    if (adding) return;
    setAdding(true);
    window.setTimeout(() => action(product, quantity, effectiveVariation), 850);
  };

  return (
    <div className="product-detail-overlay" role="dialog" aria-modal="true" aria-label={`Product details for ${product.name}`}>
      <div className="product-detail">
        <header className="product-detail__header">
          <button type="button" className="product-detail__back" onClick={onClose}>← Back</button>
          <strong>Product Details</strong>
          <button type="button" className="product-detail__close" onClick={onClose} aria-label="Close">Close</button>
        </header>
        <div className="product-detail__scroll">
          {product.enriching && (<div className="product-detail__enriching">Loading sizes, material & delivery…</div>)}
          <section className="product-detail__gallery">
            <div className="product-detail__hero"><img src={gallery[selectedImage]} alt={product.name} /></div>
            <div className="product-detail__thumbnails">
              {gallery.map((img, i) => (
                <button type="button" key={i} className={`product-detail__thumbnail${selectedImage === i ? ' is-selected' : ''}`} onClick={() => setSelectedImage(i)}>
                  <img src={img} alt="" />
                </button>
              ))}
            </div>
          </section>
          <section className="product-detail__summary">
            <div className="product-detail__title-row">
              <div>
                <p className="product-detail__seller">Seller: <strong>{(product.seller || 'MEESHO SELLER').toUpperCase()}</strong></p>
                <h2>{product.name}</h2>
              </div>
              <button type="button" className={`product-detail__save${saved ? ' is-saved' : ''}`} onClick={() => setSaved(v => !v)}>
                {saved ? <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20.8 8.7c0 5.2-8.8 10-8.8 10s-8.8-4.8-8.8-10A4.7 4.7 0 0 1 12 6.2a4.7 4.7 0 0 1 8.8 2.5Z"/></svg> : <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20.8 8.7c0 5.2-8.8 10-8.8 10s-8.8-4.8-8.8-10A4.7 4.7 0 0 1 12 6.2a4.7 4.7 0 0 1 8.8 2.5Z"/></svg>}
              </button>
            </div>
            <div className="product-detail__rating-row">
              <span className="product-detail__rating"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="m12 3 2.8 5.7 6.2.9-4.5 4.4 1.1 6.2-5.6-2.9-5.6 2.9 1.1-6.2L3 9.6l6.2-.9z"/></svg>{product.rating}</span>
              <span>{product.reviews} Ratings & Reviews</span>
            </div>
            <div className="product-detail__price-card">
              <div className="product-detail__price-row">
                <del className="product-detail__mrp">₹{mrpNumber.toLocaleString('en-IN')}</del>
                <strong className="product-detail__cod">₹{priceNumber.toLocaleString('en-IN')}</strong>
                <span className="product-detail__discount-badge">{discountPct}% OFF</span>
              </div>
              <div className="product-detail__upi-row">
                <span className="product-detail__upi-price"><svg className="product-detail__upi-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M13.2 2 5 13h6l-.8 9L19 10h-6z"/></svg><strong>₹{upiNumber.toLocaleString('en-IN')}</strong><span>with UPI</span></span>
                <span className="product-detail__upi-save">Save ₹{Math.max(priceNumber - upiNumber, 0)} with UPI</span>
              </div>
              {(ndd || product.shippingTime || product.edd) && (
                <div className="product-detail__delivery-row">
                  {ndd && <span className="product-badge product-badge--ship"><svg className="product-badge__icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M13.2 2 5 13h6l-.8 9L19 10h-6z"/></svg><span>Next Day Dispatch</span></span>}
                  {product.shippingTime && <span>{product.shippingTime}</span>}
                  {product.edd && <span>· Delivery by {product.edd}</span>}
                </div>
              )}
              {sizeOptions.length > 0 ? (
                <>
                  <div className="product-detail__size-header">
                    <strong>Select Size:</strong>
                    <span>● {effectiveVariation?.name ?? '—'} <b>{effectiveVariation && !effectiveVariation.inStock ? '(Out of stock)' : '(In Stock)'}</b></span>
                  </div>
                  <div className="product-detail__sizes">
                    {sizeOptions.map(s => (
                      <button type="button" key={s.id ?? s.name} className={effectiveVariation?.id === s.id ? 'is-selected' : ''} onClick={() => setSelectedVariation(s)}>
                        {s.name}
                      </button>
                    ))}
                  </div>
                </>
              ) : (
                <div className="product-detail__size-header"><strong>Size:</strong><span>● Free Size</span></div>
              )}
            </div>
            {attributes.length > 0 && (
              <section className="product-detail__specs">
                <h3>Material & Specifications</h3>
                <div className="product-detail__spec-grid">
                  {attributes.map((a, i) => (
                    <div key={a.field_name ?? a.display_name ?? i}>
                      <span>{a.display_name ?? a.field_name}</span>
                      <strong>{a.value}</strong>
                    </div>
                  ))}
                </div>
              </section>
            )}
            <section className="product-detail__specs">
              <h3>Product Details</h3>
              <div className="product-detail__spec-grid">
                {specs.map(([label, value]) => (<div key={label}><span>{label}</span><strong>{value}</strong></div>))}
              </div>
            </section>
            <div className="product-detail__actions">
              <button type="button" className={`cart-add-button product-detail__add${adding ? ' is-added' : ''}`} onClick={() => runCartAnimation(onAddToCart)} disabled={adding}>
                <span className="cart-add-button__circle" aria-hidden="true">
                  <span className="cart-add-button__cart"><svg viewBox="0 0 24 24"><path d="M3 4h2l2.2 10.2a2 2 0 0 0 2 1.6h7.7a2 2 0 0 0 1.9-1.4L21 8H7.1M10 20h.01M18 20h.01"/></svg></span>
                  <span className="cart-add-button__wind" />
                </span>
                <span className="cart-add-button__text">{adding ? 'Added' : 'Add to Cart'}</span>
              </button>
              <button type="button" className="product-detail__buy" onClick={() => runCartAnimation(onBuyNow)} disabled={adding}>Buy Now</button>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

// ─── Order Details Page ──────────────────────────────────────────────────────

function OrderDetailsSkeleton() {
  return (
    <div className="order-detail__skeleton" aria-hidden="true">
      <section className="order-detail__card order-detail__skeleton-card order-detail__skeleton-product">
        <div className="order-detail__skeleton-image" />
        <div className="order-detail__skeleton-copy">
          <span className="order-detail__skeleton-line is-label" />
          <span className="order-detail__skeleton-line is-title" />
          <span className="order-detail__skeleton-line is-meta" />
        </div>
        <span className="order-detail__skeleton-price" />
      </section>
      <section className="order-detail__card order-detail__skeleton-card">
        <span className="order-detail__skeleton-line is-label" />
        <span className="order-detail__skeleton-line is-wide" />
      </section>
      <section className="order-detail__card order-detail__skeleton-card order-detail__skeleton-status">
        <div><span className="order-detail__skeleton-line is-label" /><span className="order-detail__skeleton-line is-medium" /></div>
        <span className="order-detail__skeleton-line is-small" />
      </section>
      <section className="order-detail__card order-detail__skeleton-card order-detail__skeleton-tracking">
        <div className="order-detail__skeleton-heading"><span className="order-detail__skeleton-line is-label" /><span className="order-detail__skeleton-line is-medium" /></div>
        <span className="order-detail__skeleton-line is-carrier" />
        <div className="order-detail__skeleton-stepper"><span /><span /><span /><span /></div>
        <span className="order-detail__skeleton-line is-activity" />
      </section>
      <section className="order-detail__card order-detail__skeleton-card order-detail__skeleton-payment">
        <div><span className="order-detail__skeleton-line is-label" /><span className="order-detail__skeleton-line is-medium" /></div>
        <span className="order-detail__skeleton-line is-small" />
      </section>
      <section className="order-detail__card order-detail__skeleton-card">
        <span className="order-detail__skeleton-line is-label" />
        <span className="order-detail__skeleton-line is-medium" />
      </section>
      <section className="order-detail__card order-detail__skeleton-card order-detail__skeleton-address">
        <span className="order-detail__skeleton-line is-label" />
        <span className="order-detail__skeleton-line is-medium" />
        <span className="order-detail__skeleton-line is-wide" />
        <span className="order-detail__skeleton-line is-small" />
      </section>
    </div>
  );
}

function OrderDetailsPage({ order, accountId, onClose }) {
  const [details,        setDetails]        = useState(null);
  const [loadingDetails, setLoadingDetails] = useState(!!accountId);
  const [cancelReasonsOpen, setCancelReasonsOpen] = useState(false);
  const [reasons,        setReasons]        = useState([]);
  const [selectedReason, setSelectedReason] = useState(null);
  const [cancelling,     setCancelling]     = useState(false);
  const [cancelDone,     setCancelDone]     = useState(false);

  useEffect(() => {
    if (!accountId || !order.order_num) { setLoadingDetails(false); return; }
    const key = orderCacheKey(order);
    const cache = readOrdersCache();
    const cached = cache.accounts?.[accountId]?.details?.[key];
    if (cached) { setDetails(cached); setLoadingDetails(false); }
    else { setLoadingDetails(true); }

    fetchOrderDetails(accountId, order.order_num, order.sub_order_num)
      .then(r => {
        const detail = r?.data ?? r;
        if (detail) {
          setDetails(detail);
          const nextCache = readOrdersCache();
          const accountCache = nextCache.accounts?.[accountId];
          if (accountCache) {
            accountCache.details = accountCache.details ?? {};
            accountCache.details[key] = detail;
            nextCache.updatedAt = Date.now();
            writeOrdersCache(nextCache);
          }
        }
        setLoadingDetails(false);
      })
      .catch(() => setLoadingDetails(false));
  }, [accountId, order]);

  useEffect(() => {
    if (!order) return;
    const body = document.body;
    const prev = body.style.overflow;
    body.style.overflow = 'hidden';
    return () => { body.style.overflow = prev; };
  }, [order]);

  const handleOpenCancel = async () => {
    setCancelReasonsOpen(true);
    if (!reasons.length && accountId) {
      try {
        const r = await fetchCancellationReasons(accountId, order.order_num, order.sub_order_num);
        setReasons(r?.data?.cancellation_reasons ?? r?.cancellation_reasons ?? []);
      } catch {}
    }
  };

  const handleCancel = async () => {
    if (!selectedReason || !accountId) return;
    setCancelling(true);
    try {
      await cancelOrder(accountId, order.order_num, order.sub_order_num, selectedReason);
      setCancelDone(true);
    } catch {}
    setCancelling(false);
  };

  const display = details ?? order;
  const updates = (display?.shipment_updates ?? []).map(u => Array.isArray(u) ? u : [u.title ?? '', u.time ?? '']);
  const status = typeof display?.status === 'string' ? display.status : display?.status?.title?.text ?? '—';
  const tracking = { expected_delivery: display?.expected_delivery ?? '' };
  const canCancelOrder = !/delivered|cancelled/i.test(status);
  const stageIdx = /delivered/i.test(status) ? 3 : /out.?for.?delivery/i.test(status) ? 2 : /shipped/i.test(status) ? 1 : 0;
  const isCancelled = /cancel/i.test(status);

  return (
    <div className="order-detail-overlay" role="dialog" aria-modal="true">
      <div className="order-detail-page">
        <header className="order-detail__topbar">
          <button type="button" onClick={onClose}>← Back</button>
          <strong>Order Details</strong>
          <button type="button" onClick={onClose}>Close</button>
        </header>
        <div className="order-detail__scroll">
          {loadingDetails && !details ? (<OrderDetailsSkeleton />) : (
            <>
              <section className="order-detail__card order-detail__product-card">
                <div>
                  <span className="order-detail__label">Product</span>
                  {(display?.product_image || display?.product_details?.image) && (
                    <img src={display.product_image ?? display.product_details?.image} alt="" style={{width:64,height:64,borderRadius:8,objectFit:'cover',marginBottom:8}}/>
                  )}
                  <h2>{display?.product_name ?? display?.product_details?.name ?? '—'}</h2>
                  <p>Size: {display?.product_size ?? display?.product_details?.size ?? 'Free Size'} <span>•</span> Qty: {display?.product_qty ?? display?.product_details?.quantity ?? 1}</p>
                </div>
                <strong>{display?.product?.price ?? `₹${display?.amount ?? '—'}`}</strong>
              </section>

              <section className="order-detail__card order-detail__order-id">
                <span className="order-detail__label">Order ID</span>
                <strong>{display.sub_order_num ?? display.id}</strong>
              </section>

              <section className="order-detail__card order-detail__status-card">
                <div><span className="order-detail__label">Order Status</span><strong>{status}</strong></div>
                <span>{tracking?.expected_delivery ?? display.delivery ?? ''}</span>
              </section>

              <section className="order-detail__card order-detail__tracking">
                <div className="order-detail__section-heading">
                  <div><span className="order-detail__label">Tracking</span><h3>Shipment</h3></div>
                  <span>Carrier: {display.carrier ?? '—'}</span>
                </div>

                <div className="order-detail__stepper">
                  <div className="order-detail__stepper-track">
                    <span className="order-detail__stepper-progress" style={{
                      width: isCancelled ? '0%' : stageIdx === 3 ? '100%' : stageIdx === 2 ? '66.67%' : stageIdx === 1 ? '33.33%' : '0%'
                    }} />
                  </div>
                  {[['Order Placed','placed'],['Shipped','shipped'],['Out for Delivery','ofd'],['Delivered','delivered']].map(([label, key], i) => {
                    const isCompleted = isCancelled ? i === 0 : i < stageIdx;
                    const isCurrent   = !isCancelled && i === stageIdx;
                    const isCxlStep   = isCancelled && i === 1;
                    return (
                      <div className={`order-detail__step${isCompleted ? ' is-completed' : ''}${isCurrent ? ' is-current' : ''}${isCxlStep ? ' is-cancelled' : ''}`} key={key}>
                        <span className="order-detail__step-counter">{isCxlStep ? '×' : isCompleted || isCurrent ? '✓' : i + 1}</span>
                        <span className="order-detail__step-name">{isCxlStep ? 'Cancelled' : label}</span>
                      </div>
                    );
                  })}
                </div>

                {updates.length > 0 && (
                  <>
                    <button type="button" className="order-detail__tracking-toggle"
                      onClick={(e) => {
                        const open = e.currentTarget.getAttribute('aria-expanded') !== 'true';
                        e.currentTarget.setAttribute('aria-expanded', String(open));
                        e.currentTarget.classList.toggle('is-open', open);
                        e.currentTarget.nextElementSibling?.classList.toggle('is-open', open);
                      }} aria-expanded="false">
                      <span className="order-detail__tracking-toggle-left">
                        <strong>Tracking Activity</strong>
                        <span>{updates.length} updates</span>
                      </span>
                      <svg className="order-detail__tracking-arrow" viewBox="0 0 24 24" fill="none"><path d="m6 9 6 6 6-6"/></svg>
                    </button>
                    <div className="order-detail__activity-wrapper">
                      <div className="order-detail__activity-inner">
                        <div className="order-detail__activity-list">
                          {updates.map(([title, time], i) => {
                            const isDanger = /cancel|refund/i.test(title);
                            const isLatest = i === updates.length - 1;
                            return (
                              <div className="order-detail__activity-group" key={i}>
                                {i === 0 && <div className="order-detail__activity-date">Latest</div>}
                                <div className="order-detail__activity-timeline">
                                  <div className={`order-detail__activity-item${isLatest ? ' is-latest' : ''}${isDanger ? ' is-danger' : ''}`}>
                                    <span className="order-detail__activity-dot" />
                                    <div className="order-detail__activity-content">
                                      <div className="order-detail__activity-main">
                                        <strong>{title}</strong>
                                        <span>{isDanger ? 'Order status or refund update' : 'Order tracking update'}</span>
                                      </div>
                                      <time>{time}</time>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    </div>
                  </>
                )}
              </section>

              <section className="order-detail__card order-detail__payment">
                <div><span className="order-detail__label">Payment</span><strong>{display?.payment_mode ?? display?.payment_details?.final_payment_mode?.toUpperCase() ?? '—'}</strong></div>
                <strong>{display?.amount ? `₹${Number(display.amount).toLocaleString('en-IN')}` : '—'}</strong>
              </section>

              <section className="order-detail__card order-detail__seller">
                <span className="order-detail__label">Seller</span>
                <strong>{display.supplier_name ?? display.seller ?? '—'}</strong>
              </section>

              <section className="order-detail__card order-detail__address">
                <span className="order-detail__label">Delivery Address</span>
                <strong>{display.address?.name ?? '—'}</strong>
                <span>{display.address?.city ?? ''}</span>
                <span>Phone: {display.address?.phone ?? '—'}</span>
              </section>

              {canCancelOrder && !cancelDone && (
                <div className="order-detail__cancel-section">
                  {!cancelReasonsOpen ? (
                    <button type="button" className="order-detail__cancel-button" onClick={handleOpenCancel}>Cancel Order</button>
                  ) : (
                    <div className="order-detail__cancel-reasons">
                      <strong>Select a reason</strong>
                      {reasons.length === 0 && <p>Loading reasons…</p>}
                      {reasons.map(r => (
                        <button type="button" key={r.id ?? r.reason_id}
                          className={`order-detail__reason${selectedReason === (r.id ?? r.reason_id) ? ' is-selected' : ''}`}
                          onClick={() => setSelectedReason(r.id ?? r.reason_id)}>
                          {r.label ?? r.reason ?? r.text}
                        </button>
                      ))}
                      <button type="button" className="order-detail__cancel-confirm" onClick={handleCancel} disabled={!selectedReason || cancelling}>
                        {cancelling ? 'Cancelling…' : 'Confirm Cancel'}
                      </button>
                    </div>
                  )}
                </div>
              )}
              {cancelDone && (<div className="order-detail__cancel-done">Cancellation requested ✓</div>)}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

// ─── Payment Print Animation ─────────────────────────────────────────────────

function PaymentPrintAnimation({ paymentOrder }) {
  const isUpi   = paymentOrder?.paymentMethod === 'UPI';
  const amount  = Number(paymentOrder?.total || 0).toLocaleString('en-IN');
  return (
    <section className="payment-print-page">
      <div className="payment-print-intro">
        <p className="app-page__eyebrow">MesoWeb</p>
        <h2>Payment</h2>
        <p>{isUpi ? 'Scan the QR and complete your UPI payment.' : 'Your order is ready for cash on delivery.'}</p>
      </div>
      <div className="payment-print-stage">
        <div className="payment-print-printer-extension" />
        <div className="payment-print-printer">
          <div className="payment-print-printer__top"><span>MESO</span><i /></div>
          <div className="payment-print-printer__slot" />
        </div>
        <div className="payment-print-receipt-wrap">
          <article className="payment-print-receipt">
            <div className="payment-print-receipt__front">
              <header>
                <strong>MESO</strong>
                <span>{isUpi ? paymentOrder?.id : 'ORDER DETAILS'}</span>
              </header>
              {isUpi ? (
                <>
                  {paymentOrder?.qrImage ? (
                    <div className="payment-print-qr payment-print-qr--real"><img src={paymentOrder.qrImage} alt="UPI QR code" /></div>
                  ) : (
                    <div className="payment-print-qr">{Array.from({ length: 49 }, (_, i) => <i key={i} />)}</div>
                  )}
                  <div className="payment-print-upi-amount"><small>UPI PAYMENT · AMOUNT</small><strong>₹{amount}</strong></div>
                </>
              ) : (
                <div className="payment-print-order-details">
                  <div><small>ORDER ID</small><strong>{paymentOrder?.id}</strong></div>
                  <div><small>PAYMENT</small><strong>Cash on Delivery</strong></div>
                  <div><small>ITEMS</small><strong>{paymentOrder?.items?.length || 0}</strong></div>
                  {paymentOrder?.items?.map(item => (
                    <div key={item.id}><small>{item.quantity} × {item.name}</small><strong>₹{(Number((item.price||'').replace(/[^0-9]/g,'')) * item.quantity).toLocaleString('en-IN')}</strong></div>
                  ))}
                  <div className="payment-print-order-details__total"><small>TOTAL</small><strong>₹{amount}</strong></div>
                </div>
              )}
            </div>
            <div className="payment-print-receipt__back">
              <header><strong>{isUpi ? 'UPI PAYMENT' : 'CASH ON DELIVERY'}</strong><span>{paymentOrder?.id}</span></header>
              {isUpi ? (
                paymentOrder?.qrImage
                  ? <div className="payment-print-qr payment-print-qr--real"><img src={paymentOrder.qrImage} alt="UPI QR" /></div>
                  : <div className="payment-print-qr">{Array.from({ length: 49 }, (_, i) => <i key={i} />)}</div>
              ) : (
                <div className="payment-print-cod"><small>AMOUNT TO PAY</small><strong>₹{amount}</strong></div>
              )}
              <p>{isUpi ? 'Scan to pay securely' : 'Pay when your order arrives'}</p>
            </div>
          </article>
        </div>
      </div>
    </section>
  );
}

// ─── Cart Address Add Overlay ────────────────────────────────────────────────

function CartAddressAddOverlay({ accountId, onClose, onSaved }) {
  const [form, setForm] = useState({ name:'', mobile:'', pincode:'', city:'', state:'', line1:'', line2:'' });
  const [saving, setSaving] = useState(false);
  const [resolving, setResolving] = useState(false);

  const handlePincodeBlur = async () => {
    if (form.pincode.length !== 6 || !accountId) return;
    setResolving(true);
    try {
      const r = await resolveLocation(accountId, form.pincode);
      const loc = r?.data?.user_delivery_location ?? {};
      if (loc.city)  setForm(f => ({ ...f, city:  loc.city  }));
      if (loc.state) setForm(f => ({ ...f, state: loc.state }));
    } catch {}
    setResolving(false);
  };

  const canSave = accountId && form.name && form.mobile && form.pincode && form.city && form.state && form.line1;

  const handleSave = async () => {
    if (!canSave) return;
    setSaving(true);
    try {
      await createAddress({
        account_id:     accountId,
        name:           form.name,
        mobile:         form.mobile,
        pincode:        form.pincode,
        city:           form.city,
        state:          form.state,
        address_line_1: form.line1,
        address_line_2: form.line2,
        address_type:   'Home',
      });
      clearAddressCache();
      onSaved?.();
    } catch (e) {
      alert('Failed to save address.');
    }
    setSaving(false);
  };

  return (
    <div className="cart-addr-overlay" role="dialog" aria-modal="true">
      <div className="cart-addr-overlay__sheet">
        <header className="cart-addr-overlay__head">
          <strong>Add New Address</strong>
          <button type="button" onClick={onClose}>×</button>
        </header>
        <div className="cart-addr-overlay__body">
          <label><span>Full Name</span><input value={form.name} onChange={e => setForm(f => ({ ...f, name: e.target.value }))} /></label>
          <label><span>Mobile Number</span><input inputMode="tel" value={form.mobile} onChange={e => setForm(f => ({ ...f, mobile: e.target.value }))} /></label>
          <label>
            <span>Pincode {resolving ? '(resolving…)' : ''}</span>
            <input inputMode="numeric" value={form.pincode}
              onChange={e => setForm(f => ({ ...f, pincode: e.target.value.replace(/\D/g,'').slice(0,6) }))}
              onBlur={handlePincodeBlur} />
          </label>
          <div className="cart-addr-overlay__split">
            <label><span>City</span><input value={form.city} onChange={e => setForm(f => ({ ...f, city: e.target.value }))} /></label>
            <label><span>State</span><input value={form.state} onChange={e => setForm(f => ({ ...f, state: e.target.value }))} /></label>
          </div>
          <label><span>Address Line 1</span><input value={form.line1} onChange={e => setForm(f => ({ ...f, line1: e.target.value }))} /></label>
          <label><span>Address Line 2 (optional)</span><input value={form.line2} onChange={e => setForm(f => ({ ...f, line2: e.target.value }))} /></label>
        </div>
        <footer className="cart-addr-overlay__foot">
          <button type="button" className="cart-addr-overlay__cancel" onClick={onClose}>Cancel</button>
          <button type="button" className="cart-addr-overlay__save" disabled={!canSave || saving} onClick={handleSave}>
            {saving ? 'Saving…' : 'Save'}
          </button>
        </footer>
      </div>
    </div>
  );
}

// ─── Main AppPage ────────────────────────────────────────────────────────────

export default function AppPage() {
  const [active,      setActive]      = useState('home');
  const [pageReady,   setPageReady]   = useState(false);

  // accounts
  const [accounts,    setAccounts]    = useState([]);
  const [accountsLoading, setAccountsLoading] = useState(false);
  const [copiedId,    setCopiedId]    = useState(null);
  const [deleteConfirmId, setDeleteConfirmId] = useState(null);

  // home stats
  const [homeStats, setHomeStats] = useState({ total: 0, success: 0, cancelled: 0 });
  const [recentUpdates, setRecentUpdates] = useState([]);

  // dash stats
  const [dashStats, setDashStats]             = useState(null);
  const [statsReady, setStatsReady]           = useState(false);
  const [statsRefreshing, setStatsRefreshing] = useState(false);
  const refreshPollRef                        = useRef(null);

  // addresses page
  const [addressAccountId,  setAddressAccountId]  = useState('');
  const [addressList,       setAddressList]        = useState([]);
  const [addressLoading,    setAddressLoading]     = useState(false);
  const [addressForm,       setAddressForm]        = useState({ name:'', mobile:'', pincode:'', city:'', state:'', line1:'', line2:'', isDefault: false });
  const [addressSaved,      setAddressSaved]       = useState(false);
  const [addressSaving,     setAddressSaving]      = useState(false);
  const [pincodeResolving,  setPincodeResolving]   = useState(false);
  const [addrCacheRefreshing, setAddrCacheRefreshing] = useState(false);
  const addrPollRef = useRef(null);

  // add-account / FOD / OTP
  const [phoneNumber,      setPhoneNumber]      = useState('');
  const [accountSubmitted, setAccountSubmitted] = useState(false);
  const [fodLoading,       setFodLoading]       = useState(false);
  const [fodReady,         setFodReady]         = useState(false);
  const [fodResult,        setFodResult]        = useState(null);
  const [fodLoader,        setFodLoader]        = useState(0);
  const [fodLoaderVisible, setFodLoaderVisible] = useState(true);
  const [referralEditAttempt, setReferralEditAttempt] = useState(false);
  const [mshoStatus,       setMshoStatus]       = useState('bad');

  const [otpState,     setOtpState]     = useState(null);
  const [otpCode,      setOtpCode]      = useState('');
  const [otpSending,   setOtpSending]   = useState(false);
  const [otpVerifying, setOtpVerifying] = useState(false);
  const [otpError,     setOtpError]     = useState('');
  const [loginSuccess, setLoginSuccess] = useState(false);

  // search
  const [searchQuery,     setSearchQuery]     = useState('');
  const [searchAccountId, setSearchAccountId] = useState('');
  const [searchSubmitted, setSearchSubmitted] = useState(false);
  const [searchResults,   setSearchResults]   = useState([]);
  const [searchLoading,   setSearchLoading]   = useState(false);

  // fyp
  const [fypAccountId,  setFypAccountId]  = useState('anonymous');
  const [fypProducts,   setFypProducts]   = useState([]);
  const [fypLoading,    setFypLoading]    = useState(false);

  // product detail
  const [selectedProduct, setSelectedProduct] = useState(null);

  // cart
  const [cartAccountId,   setCartAccountId]   = useState('');
  const [cartItems,       setCartItems]        = useState([]);
  const [cartJustAddedId, setCartJustAddedId]  = useState(null);
  const [fetchedProduct,  setFetchedProduct]   = useState(null);
  const [productLink,     setProductLink]       = useState('');
  const [fetchingProduct, setFetchingProduct]  = useState(false);
  const [productAdded,    setProductAdded]      = useState(false);
  const [productMovingToCart, setProductMovingToCart] = useState(false);
  const [productDisappearing, setProductDisappearing] = useState(false);

  // cart address binding
  const [cartAddresses,     setCartAddresses]     = useState([]);
  const [cartAddressId,     setCartAddressId]     = useState(null);
  const [cartBoundSession,  setCartBoundSession]  = useState(null);
  const [cartAddressLoading, setCartAddressLoading] = useState(false);
  const [showCartAddressOverlay, setShowCartAddressOverlay] = useState(false);

  // checkout
  const [checkoutStep,    setCheckoutStep]     = useState(false);
  const [savedAddresses,  setSavedAddresses]   = useState([]);
  const [selectedAddressId, setSelectedAddressId] = useState(null);

  // payment
  const [paymentMethod,   setPaymentMethod]   = useState('UPI');
  const [paymentPage,     setPaymentPage]     = useState(false);
  const [paymentOrder,    setPaymentOrder]    = useState(null);
  const [paymentChecking, setPaymentChecking] = useState(false);
  const [orderPlacedOverlay, setOrderPlacedOverlay] = useState(false);

  // orders
  const [ordersAccountId, setOrdersAccountId] = useState('all');
  const [ordersList,      setOrdersList]      = useState([]);
  const [ordersLoading,   setOrdersLoading]   = useState(false);
  const [ordersRetrying,  setOrdersRetrying]  = useState(false);
  const [selectedOrder,   setSelectedOrder]   = useState(null);

  // profile giphy
  const [profileAvatarUrl,     setProfileAvatarUrl]     = useState('https://media0.giphy.com/media/v1.Y2lkPTZjMDliOTUyYms4NDNneHE1cjB4anJmYmZsMjE0cXE1MTA0cWVsZzhxcWpkMnV1OSZlcD12MV9zdGlja2Vyc19zZWFyY2gmY3Q9cw/rHG8ao0mYEKdlKvb8T/source.gif');
  const [profileAvatarVisible, setProfileAvatarVisible] = useState(true);
  const [homeCreditAvatarUrl,  setHomeCreditAvatarUrl]  = useState('https://media0.giphy.com/media/v1.Y2lkPTZjMDliOTUyYms4NDNneHE1cjB4anJmYmZsMjE0cXE1MTA0cWVsZzhxcWpkMnV1OSZlcD12MV9zdGlja2Vyc19zZWFyY2gmY3Q9cw/rHG8ao0mYEKdlKvb8T/source.gif');
  const [homeCreditAvatarVisible, setHomeCreditAvatarVisible] = useState(true);

  // Reset payment on tab switch
  useEffect(() => { setPaymentPage(false); setPaymentOrder(null); }, [active]);

  // Load accounts
  const loadAccounts = useCallback(async () => {
    setAccountsLoading(true);
    try {
      const res = await fetchAccounts();
      const list = res.accounts ?? [];
      setAccounts(list);

      const total     = list.length;
      const success   = list.filter(a => ['Delivered','Shipped','Out for Delivery'].includes(a.last_order_status)).length;
      const cancelled = list.filter(a => a.last_order_status === 'Cancelled').length;
      setHomeStats({ total, success, cancelled });

      const snapshot = loadOrderSnapshot();
      const changes  = [];
      for (const acc of list) {
        const key  = acc.account_id;
        const prev = snapshot[key];
        const next = acc.last_order_status ?? '—';
        if (prev && prev !== next) {
          changes.push({ phone: acc.phone, prev, next, time: 'Just now' });
        }
        snapshot[key] = next;
      }
      saveOrderSnapshot(snapshot);
      if (changes.length) setRecentUpdates(prev => [...changes, ...prev].slice(0, 10));
    } catch (e) { console.error('[accounts]', e); }
    setAccountsLoading(false);
  }, []);

  useEffect(() => { loadAccounts(); }, [loadAccounts]);

  // Dashboard stats
  const loadDashboardStats = useCallback(async () => {
    try {
      const res = await fetchDashboardStats();
      if (res.ready) { setDashStats(res.stats); setStatsReady(true); }
    } catch (e) { console.error('[dashStats]', e); }
  }, []);

  const loadRecentUpdates = useCallback(async () => {
    try {
      const res = await fetchRecentUpdates();
      if (res.success && res.events?.length) {
        setRecentUpdates(res.events.map(ev => ({
          phone: ev.phone, prev: ev.old_status, next: ev.new_status,
          time:  new Date(ev.timestamp).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' }),
        })).slice(0, 5));
      }
    } catch (e) { console.error('[recentUpdates]', e); }
  }, []);

  const handleStatsRefresh = useCallback(async () => {
    if (statsRefreshing) return;
    setStatsRefreshing(true);
    try {
      await triggerStatsRefresh();
      if (refreshPollRef.current) window.clearInterval(refreshPollRef.current);
      refreshPollRef.current = window.setInterval(async () => {
        try {
          const s = await fetchRefreshStatus();
          if (!s.running) {
            window.clearInterval(refreshPollRef.current);
            refreshPollRef.current = null;
            setStatsRefreshing(false);
            await loadDashboardStats();
            await loadRecentUpdates();
          }
        } catch {}
      }, 3000);
    } catch (e) {
      console.error('[statsRefresh]', e);
      setStatsRefreshing(false);
    }
  }, [statsRefreshing, loadDashboardStats, loadRecentUpdates]);

  useEffect(() => {
    if (active !== 'home') return;
    loadDashboardStats();
    loadRecentUpdates();
    return () => {
      if (refreshPollRef.current) { window.clearInterval(refreshPollRef.current); refreshPollRef.current = null; }
    };
  }, [active, loadDashboardStats, loadRecentUpdates]);

  // Addresses page load
  useEffect(() => {
    if (!addressAccountId) return;
    let cancelled = false;
    setAddressLoading(true);
    setAddressList([]);

    const applyAddresses = (items) => {
      if (cancelled) return;
      const addrs = Array.isArray(items) ? items : [];
      setAddressList(addrs);
      if (addrs.length) setSelectedAddressId(addrs.find(a => a.is_default)?.id ?? addrs[0].id);
    };

    fetchAddressCache()
      .then(res => {
        const accountsMap = res?.accounts ?? res?.data?.accounts;
        const entry = accountsMap?.[addressAccountId];
        const cached = entry?.addresses;
        if (Array.isArray(cached) && cached.length) { applyAddresses(cached); return; }
        return fetchAddresses(addressAccountId).then(r => applyAddresses(r?.data?.addresses ?? r?.addresses ?? []));
      })
      .catch(() => fetchAddresses(addressAccountId).then(r => applyAddresses(r?.data?.addresses ?? r?.addresses ?? [])).catch(() => {}))
      .finally(() => { if (!cancelled) setAddressLoading(false); });

    return () => { cancelled = true; };
  }, [addressAccountId]);

  const handleAddressRefresh = useCallback(async () => {
    if (addrCacheRefreshing) return;
    setAddrCacheRefreshing(true);
    clearAddressCache();
    try {
      await triggerAddressFetch();
      if (addrPollRef.current) window.clearInterval(addrPollRef.current);
      addrPollRef.current = window.setInterval(async () => {
        try {
          const status = await fetchAddressFetchStatus();
          if (status?.running === false || status?.data?.running === false) {
            window.clearInterval(addrPollRef.current);
            addrPollRef.current = null;
            clearAddressCache();
            if (addressAccountId) {
              setAddressLoading(true);
              try {
                const res = await fetchAddressCache();
                const accountsMap = res?.accounts ?? res?.data?.accounts;
                const addrs = accountsMap?.[addressAccountId]?.addresses ?? [];
                setAddressList(Array.isArray(addrs) ? addrs : []);
                if (addrs.length) setSelectedAddressId(addrs.find(a => a.is_default)?.id ?? addrs[0].id);
              } catch {} finally { setAddressLoading(false); }
            }
            setAddrCacheRefreshing(false);
          }
        } catch {}
      }, 3000);
    } catch { setAddrCacheRefreshing(false); }
  }, [addrCacheRefreshing, addressAccountId]);

  useEffect(() => () => {
    if (addrPollRef.current) { window.clearInterval(addrPollRef.current); addrPollRef.current = null; }
  }, []);

  const handlePincodeBlur = async () => {
    if (addressForm.pincode.length !== 6 || !addressAccountId) return;
    setPincodeResolving(true);
    try {
      const r = await resolveLocation(addressAccountId, addressForm.pincode);
      const loc = r?.data?.user_delivery_location ?? {};
      if (loc.city)  setAddressForm(f => ({ ...f, city:  loc.city  }));
      if (loc.state) setAddressForm(f => ({ ...f, state: loc.state }));
    } catch {}
    setPincodeResolving(false);
  };

  // FOD hunt
  useEffect(() => {
    if (active !== 'add-account' || phoneNumber.length !== 10 || !accountSubmitted) return;
    setMshoStatus('bad');
    setFodLoading(true);
    setFodReady(false);
    setFodLoader(0);
    setFodResult(null);
    setOtpState(null);
    setLoginSuccess(false);

    let cancelled = false;
    const loaderTimer = window.setInterval(() => {
      setFodLoaderVisible(false);
      window.setTimeout(() => { setFodLoader(c => c + 1); setFodLoaderVisible(true); }, 180);
    }, 1000);

    runFodHunt('2560ev').then(result => {
      if (cancelled) return;
      setFodResult(result);
      setFodLoading(false);
      setFodReady(true);
      window.clearInterval(loaderTimer);
      window.setTimeout(() => document.querySelector('.fod-results')?.scrollIntoView({ behavior: 'smooth', block: 'start' }), 60);
    }).catch(err => {
      if (cancelled) return;
      console.error('[FOD]', err);
      setFodLoading(false);
      window.clearInterval(loaderTimer);
    });

    return () => { cancelled = true; window.clearInterval(loaderTimer); };
  }, [active, phoneNumber, accountSubmitted]);

  const handleSendOtp = async () => {
    if (!fodResult || !phoneNumber) return;
    setOtpSending(true); setOtpError('');
    const iid  = genHex(32);
    const sid  = genUUID();
    const gaid = genUUID();
    const shid = genUUID();
    const anon_xo = fodResult.best_xo ?? '';
    try {
      const r = await sendOtp({
        phone: phoneNumber, instance_id: iid, app_session_id: sid,
        gaid, shield_session_id: shid, anon_xo,
        fod_bucket: fodResult.max_fod_bucket ?? 0, via: '2560ev',
      });
      if (!r.success) throw new Error(r.error ?? 'OTP send failed');
      setOtpState({ ...r, iid, sid, gaid, shid, anon_xo });
    } catch (e) { setOtpError(String(e.message)); }
    setOtpSending(false);
  };

  const handleVerifyOtp = async () => {
    if (!otpState || !otpCode) return;
    setOtpVerifying(true); setOtpError('');
    try {
      const r = await verifyOtp({
        phone: phoneNumber, otp: otpCode, state: otpState.state,
        channel_auth_token: otpState.channel_auth_token, uid: otpState.uid,
        ts_id: otpState.ts_id, in_id: otpState.in_id, as_id: otpState.as_id,
        instance_id: otpState.iid, app_session_id: otpState.sid,
        gaid: otpState.gaid, shield_session_id: otpState.shid,
        anon_xo: otpState.anon_xo,
        fod_bucket: fodResult?.max_fod_bucket ?? 0, via: '2560ev',
      });
      if (!r.success) throw new Error(r.error ?? 'OTP verification failed');
      setLoginSuccess(true);
      loadAccounts();
    } catch (e) { setOtpError(String(e.message)); }
    setOtpVerifying(false);
  };

  const handleSearch = async (e) => {
    e.preventDefault();
    if (!searchQuery.trim()) return;
    setSearchLoading(true);
    setSearchSubmitted(true);
    setSearchResults([]);
    try {
      const r    = await searchProducts(searchQuery, { accountId: searchAccountId || null });
      const list = parseSearchWidgets(r);
      setSearchResults(list);
    } catch (err) { console.error('[search]', err); }
    setSearchLoading(false);
  };

  // FYP
  useEffect(() => {
    if (active !== 'fyp') return;
    setFypLoading(true);
    setFypProducts([]);
    const accId = fypAccountId === 'anonymous' ? null : fypAccountId;
    const feed = accId ? searchProducts('women fashion', { accountId: accId, mallEnabled: true }) : fetchAnonForYou(40);
    feed.then(r => setFypProducts(parseSearchWidgets(r))).catch(() => {}).finally(() => setFypLoading(false));
  }, [active, fypAccountId]);

  // Cart address: load when account changes
  useEffect(() => {
    if (!cartAccountId) { setCartAddresses([]); setCartAddressId(null); setCartBoundSession(null); return; }
    let cancelled = false;
    setCartAddressLoading(true);

    fetchAddresses(cartAccountId)
      .then(r => {
        if (cancelled) return;
        const addrs = r?.data?.addresses ?? r?.addresses ?? [];
        setCartAddresses(Array.isArray(addrs) ? addrs : []);
        const def = addrs.find(a => a.is_default) ?? addrs[0];
        if (def) setCartAddressId(def.id);
      })
      .catch(() => { if (!cancelled) setCartAddresses([]); })
      .finally(() => { if (!cancelled) setCartAddressLoading(false); });

    // Reset session so it re-binds
    setCartBoundSession(null);
    return () => { cancelled = true; };
  }, [cartAccountId]);

  // Cart: fetch product from link
  const handleFetchProduct = async () => {
    const pid = extractProductId(productLink);
    if (!pid) { alert('Could not parse product ID from link. Try pasting the full meesho.com URL.'); return; }
    setFetchingProduct(true);
    setFetchedProduct(null);
    try {
      const raw    = await fetchProductDynamic(pid, cartAccountId || null);
      const parsed = parseProductDynamic(raw);
      if (parsed) setFetchedProduct(parsed);
      else alert('Product not found.');
    } catch { alert('Failed to fetch product.'); }
    setFetchingProduct(false);
  };

  const handleFetchMyCart = async () => {
    if (!cartAccountId) { alert('Select an account first.'); return; }
    try {
      const session = cartBoundSession || null;
      const full   = await fetchCartDetails(cartAccountId, { cartSession: session });
      const result = full?.result ?? full?.data?.result ?? {};
      const splits = result?.splits ?? [];
      const returnedSession = full?.cart_session ?? full?.data?.cart_session ?? session;

      if (returnedSession && returnedSession !== cartBoundSession) {
        setCartBoundSession(returnedSession);
      }

      if (!splits.length) { alert('Cart is empty.'); setCartItems([]); return; }

      const items = splits.flatMap(split =>
        (split.products ?? []).map(p => ({
          id:          p.product_id,
          identifier:  p.identifier,
          catalogId:   p.catalog?.id ?? p.product_id,
          productId:   p.product_id,
          supplierId:  split.supplier?.id,
          variationId: p.variation_id ?? 167,
          name:        p.name ?? p.catalog?.name ?? 'Product',
          category:    p.category?.sub_sub_category_name ?? '',
          price:       `₹${Number(p.price ?? 0).toLocaleString('en-IN')}`,
          image:       (p.images ?? [])[0] ?? '',
          quantity:    p.quantity ?? 1,
          cartSession: returnedSession,
        }))
      );
      setCartItems(items);
    } catch (e) {
      console.error('[fetchMyCart]', e);
      alert('Failed to fetch cart.');
    }
  };

  const handleBindCartAddress = async (addressId, pin) => {
    if (!cartAccountId || !addressId) return;
    try {
      const r = await bindAddressToCart(cartAccountId, addressId, pin);
      if (r?.success && r.cart_session) {
        setCartBoundSession(r.cart_session);
        setCartAddressId(r.address_id ?? addressId);
      }
    } catch (e) { console.error('[bindAddress]', e); }
  };

  const removeCartItem = async (item) => {
    if (cartAccountId && item.identifier) {
      try { await removeFromCart(cartAccountId, [item.identifier], cartBoundSession ?? item.cartSession ?? null); } catch (e) { console.error('[removeCartItem]', e); }
    }
    setCartItems(items => items.filter(i => i.id !== item.id));
  };

  const changeCartQuantity = async (item, delta) => {
    const next = item.quantity + delta;
    if (next <= 0) { await removeCartItem(item); return; }

    if (delta < 0 && cartAccountId && item.identifier) {
      try { await removeFromCart(cartAccountId, [item.identifier], cartBoundSession ?? item.cartSession ?? null); } catch (e) { console.error('[changeQty remove]', e); }
      if (item.productId && item.supplierId) {
        try {
          const r = await addToCart(cartAccountId, {
            product_id: item.productId, supplier_id: item.supplierId,
            variation_id: item.variationId ?? 167, variation: 'Free Size',
            quantity: next, price_type_id: 'basic_return_price',
            cart_session: cartBoundSession ?? item.cartSession ?? null,
          });
          const newIdentifier = r?.result?.splits?.[0]?.products?.[0]?.identifier ?? item.identifier;
          const newSession = r?.cart_session ?? cartBoundSession ?? item.cartSession;
          setCartItems(items => items.map(i => i.id === item.id ? { ...i, quantity: next, identifier: newIdentifier, cartSession: newSession } : i));
          return;
        } catch (e) { console.error('[changeQty re-add]', e); }
      }
    }

    if (delta > 0 && cartAccountId && item.productId && item.supplierId) {
      try {
        const r = await addToCart(cartAccountId, {
          product_id: item.productId, supplier_id: item.supplierId,
          variation_id: item.variationId ?? 167, variation: 'Free Size',
          quantity: 1, price_type_id: 'basic_return_price',
          cart_session: cartBoundSession ?? item.cartSession ?? null,
        });
        const newIdentifier = r?.result?.splits?.[0]?.products?.[0]?.identifier ?? item.identifier;
        const newSession = r?.cart_session ?? cartBoundSession ?? item.cartSession;
        setCartItems(items => items.map(i => i.id === item.id ? { ...i, quantity: next, identifier: newIdentifier, cartSession: newSession } : i));
        return;
      } catch (e) { console.error('[changeQty add]', e); }
    }

    setCartItems(items => items.map(i => i.id === item.id ? { ...i, quantity: next } : i));
  };

  const addFetchedToCart = async () => {
    if (!fetchedProduct) return;
    if (!cartAccountId) { alert('Select an account first.'); return; }

    setProductAdded(true);

    try {
      const r = await addToCart(cartAccountId, {
        product_id: fetchedProduct.productId,
        supplier_id: fetchedProduct.supplierId,
        variation_id: fetchedProduct.variationId ?? 167,
        variation: 'Free Size',
        quantity: 1,
        price_type_id: 'basic_return_price',
        cart_session: cartBoundSession,
      });
      const newSession = r?.cart_session ?? cartBoundSession;
      if (newSession) setCartBoundSession(newSession);
    } catch (e) { console.error('[addFetchedToCart]', e); }

    window.setTimeout(() => {
      setProductMovingToCart(true);
      setCartItems(items =>
        items.some(i => i.id === fetchedProduct.id)
          ? items
          : [...items, { ...fetchedProduct, quantity: 1, cartSession: cartBoundSession }]
      );
      setCartJustAddedId(fetchedProduct.id);
      window.setTimeout(() => setCartJustAddedId(null), 1100);
    }, 1000);

    window.setTimeout(() => setProductDisappearing(true), 1000);
    window.setTimeout(() => {
      setFetchedProduct(null);
      setProductAdded(false);
      setProductMovingToCart(false);
      setProductDisappearing(false);
      setProductLink('');
    }, 1700);
  };

  const openProductDetail = useCallback(async (lean) => {
    if (!lean) return;
    setSelectedProduct({ ...lean, enriching: true });
    const pid = String(lean.productId ?? lean.catalogId ?? '');
    if (!pid) { setSelectedProduct(prev => (prev ? { ...prev, enriching: false } : prev)); return; }
    const accId = searchAccountId || (fypAccountId && fypAccountId !== 'anonymous' ? fypAccountId : null);
    try {
      const [s, d] = await Promise.allSettled([
        fetchProductStatic(pid, accId),
        fetchProductDynamic(pid, accId),
      ]);
      const rich = s.status === 'fulfilled' ? parseProductStatic(s.value) : null;
      const dyn  = d.status === 'fulfilled' ? parseProductDynamic(d.value) : null;
      setSelectedProduct(prev => {
        if (!prev || prev.catalogId !== lean.catalogId) return prev;
        return { ...prev, ...(rich ?? {}), ...(dyn ?? {}), enriching: false };
      });
    } catch (e) {
      console.error('[openProductDetail]', e);
      setSelectedProduct(prev => (prev ? { ...prev, enriching: false } : prev));
    }
  }, [searchAccountId, fypAccountId]);

  const addProductToCart = async (product, quantity = 1, goToCart = false, variation = null) => {
    if (cartAccountId) {
      try {
        const r = await addToCart(cartAccountId, {
          product_id: product.productId,
          supplier_id: product.supplierId,
          variation_id: variation?.id ?? product.variationId ?? 167,
          variation: variation?.name ?? 'Free Size',
          quantity,
          price_type_id: product.priceTypeId || 'basic_return_price',
          cart_session: cartBoundSession,
        });
        const newSession = r?.cart_session ?? cartBoundSession;
        if (newSession) setCartBoundSession(newSession);
      } catch (e) { console.error('[addProductToCart]', e); }
    }

    setCartItems(items => {
      const existing = items.find(i => i.id === product.id);
      if (existing) return items.map(i => i.id === product.id ? { ...i, quantity: i.quantity + quantity } : i);
      return [...items, { ...product, quantity, cartSession: cartBoundSession }];
    });

    setCartJustAddedId(product.id);
    window.setTimeout(() => setCartJustAddedId(c => c === product.id ? null : c), 1100);
    setSelectedProduct(null);
    if (goToCart) setActive('cart');
  };

  // Checkout: address list snapshot (read only)
  useEffect(() => {
    if (!checkoutStep || !cartAccountId) return;
    let cancelled = false;
    const applyAddresses = (items) => {
      if (cancelled) return;
      const addrs = Array.isArray(items) ? items : [];
      setSavedAddresses(addrs);
      if (addrs.length) {
        // Prefer the one already bound to the cart
        const bound = addrs.find(a => a.id === cartAddressId);
        setSelectedAddressId(bound?.id ?? addrs.find(a => a.is_default)?.id ?? addrs[0].id);
      }
    };
    fetchAddresses(cartAccountId)
      .then(r => applyAddresses(r?.data?.addresses ?? r?.addresses ?? []))
      .catch(() => {});
    return () => { cancelled = true; };
  }, [checkoutStep, cartAccountId, cartAddressId]);

  const selectedAddress = savedAddresses.find(a => a.id === selectedAddressId) ?? savedAddresses[0] ?? null;

  const cartSubtotal = cartItems.reduce((t, i) => t + Number((i.price||'').replace(/[^0-9]/g,'')) * i.quantity, 0);
  const fodDiscount  = cartItems.length ? 300 : 0;
  const codTotal     = Math.max(cartSubtotal - fodDiscount, 0) + 50;
  const upiTotal     = Math.max(cartSubtotal - fodDiscount, 0);

  const startPaymentPage = async () => {
    const orderId = `MW-${Date.now().toString().slice(-8)}`;
    setPaymentChecking(false);

    if (cartAccountId && cartItems.length && cartAddressId) {
      const item = cartItems[0];
      try {
        const res = await processCheckout({
          account_id:              cartAccountId,
          product_id:              item.productId,
          supplier_id:             item.supplierId,
          variation_id:            item.variationId ?? 167,
          variation:               'Free Size',
          quantity:                item.quantity,
          selected_price_type_id:  'basic_return_price',
          address_id:              cartAddressId,
          pincode:                 String(selectedAddress?.pin ?? selectedAddress?.pincode ?? '445202'),
          payment_mode:            paymentMethod.toLowerCase(),
          cancel_after_place:      false,
          cart_session:            cartBoundSession,
        });
        setPaymentOrder({
          id:            res.order_num ?? orderId,
          subOrderNum:   res.sub_order_num ?? '',
          items:         cartItems.map(i => ({ ...i })),
          total:         paymentMethod === 'UPI' ? upiTotal : codTotal,
          paymentMethod,
          address:       selectedAddress,
          qrImage:       res.qr_image ?? null,
          intentUrl:     res.intent_url ?? null,
          paymentLightColor: Math.random() < 0.5 ? 'red' : 'green',
        });
      } catch {
        setPaymentOrder({
          id: orderId, items: cartItems.map(i => ({ ...i })),
          total: paymentMethod === 'UPI' ? upiTotal : codTotal,
          paymentMethod, address: selectedAddress,
          qrImage: null, paymentLightColor: 'green',
        });
      }
    } else {
      setPaymentOrder({
        id: orderId, items: cartItems.map(i => ({ ...i })),
        total: paymentMethod === 'UPI' ? upiTotal : codTotal,
        paymentMethod, address: selectedAddress,
        qrImage: null, paymentLightColor: 'green',
      });
    }

    setCheckoutStep(false);
    setPaymentPage(true);
  };

  const completeOrder = () => {
    if (!paymentOrder) return;
    setOrderPlacedOverlay(true);
    window.setTimeout(() => {
      setOrderPlacedOverlay(false);
      setPaymentPage(false);
      setPaymentOrder(null);
      setCartItems([]);
      setActive('home');
    }, 1600);
  };

  // Orders
  useEffect(() => {
    if (active !== 'orders' || !accounts.length) return;
    let cancelled = false;
    let cache = readOrdersCache();

    const showCached = () => {
      if (!cancelled) {
        const cached = getCachedOrdersForAccounts(cache, accounts, ordersAccountId);
        setOrdersList(cached);
        setOrdersLoading(cached.length === 0);
      }
    };

    const refreshAll = async () => {
      if (cancelled) return;
      setOrdersRetrying(true);
      const accountJobs = accounts.map(async (acc) => {
        try {
          const r = await fetchOrders(acc.account_id);
          const freshOrders = (r.orders ?? []).map(o => ({ ...o, phone: acc.phone, accountId: acc.account_id }));
          cache = mergeCachedOrders(cache, acc.account_id, acc.phone, freshOrders);
          writeOrdersCache(cache);
          if (!cancelled) {
            const nextOrders = getCachedOrdersForAccounts(cache, accounts, ordersAccountId);
            setOrdersList(nextOrders);
            setOrdersLoading(nextOrders.length === 0);
          }
          for (const order of freshOrders) {
            if (cancelled) return;
            const key = orderCacheKey(order);
            if (!key) continue;
            try {
              const detailResponse = await fetchOrderDetails(acc.account_id, order.order_num, order.sub_order_num);
              const detail = detailResponse?.data ?? detailResponse;
              if (detail) {
                const accountCache = cache.accounts[acc.account_id] ?? { phone: acc.phone, orders: [], details: {} };
                accountCache.details = accountCache.details ?? {};
                accountCache.details[key] = detail;
                cache.accounts[acc.account_id] = accountCache;
                cache.updatedAt = Date.now();
                writeOrdersCache(cache);
              }
            } catch {}
          }
        } catch (e) { console.error('[orders refresh]', acc.account_id, e); }
      });
      await Promise.all(accountJobs);
      if (!cancelled) {
        const finalOrders = getCachedOrdersForAccounts(cache, accounts, ordersAccountId);
        setOrdersList(finalOrders);
        setOrdersLoading(finalOrders.length === 0);
        setOrdersRetrying(false);
      }
    };

    showCached();
    void refreshAll();

    const retryTimer = window.setInterval(() => {
      if (cancelled) return;
      const current = getCachedOrdersForAccounts(readOrdersCache(), accounts, ordersAccountId);
      if (!current.length) void refreshAll();
    }, 15000);

    const timer = window.setInterval(() => {
      cache = readOrdersCache();
      void refreshAll();
    }, 5 * 60 * 1000);

    return () => {
      cancelled = true;
      window.clearInterval(retryTimer);
      window.clearInterval(timer);
    };
  }, [active, accounts]);

  useEffect(() => {
    if (active !== 'orders' || !accounts.length) return;
    const cache = readOrdersCache();
    const cached = getCachedOrdersForAccounts(cache, accounts, ordersAccountId);
    setOrdersList(cached);
    if (cached.length) setOrdersLoading(false);
  }, [active, ordersAccountId, accounts]);

  // Giphy home
  useEffect(() => {
    if (active !== 'home') return;
    let cancelled = false, offset = Math.floor(Math.random() * 12) * 10, fetching = false, timer = null;
    const seen = new Set(), queue = [];
    const KEY  = 'SSFO9NLIyMofQe24akeyd88xjSMV3jl5';
    const fetchBatch = async () => {
      if (cancelled || fetching) return; fetching = true;
      try {
        const r = await fetch(`https://api.giphy.com/v1/stickers/search?api_key=${KEY}&q=Clash+Royale+emotes&limit=10&offset=${offset}&rating=pg&fields=id,images`);
        const d = await r.json();
        (d.data ?? []).filter(s => s?.id && !seen.has(s.id) && (s?.images?.fixed_height?.url || s?.images?.downsized?.url)).forEach(s => { seen.add(s.id); queue.push(s.images.fixed_height?.url || s.images.downsized.url); });
        offset += 10;
      } catch {} finally { fetching = false; }
    };
    const rotate = () => {
      if (cancelled) return;
      if (queue.length) { setHomeCreditAvatarVisible(false); const u = queue.shift(); window.setTimeout(() => { if (!cancelled) setHomeCreditAvatarUrl(u); }, 180); }
      if (queue.length <= 5) void fetchBatch();
    };
    void fetchBatch();
    timer = window.setInterval(rotate, 5000);
    return () => { cancelled = true; if (timer) window.clearInterval(timer); };
  }, [active]);

  useEffect(() => {
    if (!homeCreditAvatarUrl) return;
    const img = new Image(); img.src = homeCreditAvatarUrl;
    img.onload = () => setHomeCreditAvatarVisible(true);
    return () => { img.onload = null; };
  }, [homeCreditAvatarUrl]);

  useEffect(() => {
    if (active !== 'profile') return;
    let cancelled = false;
    let offset = 0;
    let batch = [];
    let batchIndex = 0;
    let totalCount = Infinity;
    let fetching = false;
    let rotateTimer = null;
    let batchTimer = null;
    const seen = new Set();
    const KEY = 'SSFO9NLIyMofQe24akeyd88xjSMV3jl5';
    const BATCH_SIZE = 500;
    const ROTATE_MS = 5000;
    const BATCH_WINDOW_MS = 2 * 60 * 60 * 1000;

    const showNext = () => {
      if (cancelled || !batch.length) return;
      const u = batch[batchIndex % batch.length];
      batchIndex += 1;
      setProfileAvatarVisible(false);
      window.setTimeout(() => { if (!cancelled) setProfileAvatarUrl(u); }, 180);
    };

    const fetchBatch = async () => {
      if (cancelled || fetching || offset >= totalCount) return;
      fetching = true;
      try {
        const nextBatch = [];
        let pagesFetched = 0;
        while (!cancelled && nextBatch.length < BATCH_SIZE && offset < totalCount && pagesFetched < 20) {
          const remaining = BATCH_SIZE - nextBatch.length;
          const limit = Math.min(50, remaining);
          const r = await fetch(`https://api.giphy.com/v1/stickers/search?api_key=${KEY}&q=Clash+Royale+emotes&limit=${limit}&offset=${offset}&rating=pg`);
          const d = await r.json();
          totalCount = Number.isFinite(d?.pagination?.total_count) ? d.pagination.total_count : totalCount;
          const fresh = (d.data ?? []).filter(s => s?.id && !seen.has(s.id) && (s?.images?.fixed_height?.url || s?.images?.downsized?.url));
          fresh.forEach(s => { seen.add(s.id); nextBatch.push(s.images.fixed_height?.url || s.images.downsized.url); });
          const returned = Array.isArray(d.data) ? d.data.length : 0;
          if (!returned) break;
          offset += returned;
          pagesFetched += 1;
          if (returned < limit) break;
        }
        if (!cancelled && nextBatch.length) {
          batch = nextBatch;
          batchIndex = 0;
          showNext();
          if (batchTimer) window.clearTimeout(batchTimer);
          batchTimer = window.setTimeout(() => { if (!cancelled) void fetchBatch(); }, BATCH_WINDOW_MS);
        }
      } catch {} finally { fetching = false; }
    };

    void fetchBatch();
    rotateTimer = window.setInterval(() => { if (cancelled) return; if (batch.length) showNext(); }, ROTATE_MS);
    return () => {
      cancelled = true;
      if (rotateTimer) window.clearInterval(rotateTimer);
      if (batchTimer) window.clearTimeout(batchTimer);
    };
  }, [active]);

  useEffect(() => {
    if (!profileAvatarUrl) return;
    const img = new Image(); img.src = profileAvatarUrl;
    img.onload = () => setProfileAvatarVisible(true);
    return () => { img.onload = null; };
  }, [profileAvatarUrl]);

  const handleReferralAttempt = (e) => {
    e.preventDefault();
    setReferralEditAttempt(true);
    window.setTimeout(() => setReferralEditAttempt(false), 1000);
  };

  const handlePhoneChange = (e) => {
    const next = e.target.value.replace(/\D/g,'').slice(0,10);
    setPhoneNumber(next);
    if (next.length < 10) { setMshoStatus('bad'); setAccountSubmitted(false); setFodLoading(false); setFodReady(false); setFodLoader(0); }
  };

  const activeItem = navItems.find(i => i.id === active);
  const actionCards = [
    { id: 'accounts',    label: 'Accounts',    value: String(homeStats.total),     detail: 'Total Meesho Accounts',      icon: '▥' },
    { id: 'addresses',   label: 'Addresses',   value: '—',                          detail: 'Manage Delivery Addresses',   icon: '⌂' },
    { id: 'add-account', label: 'ADD Account', value: '—',                          detail: 'Add Meesho Account',          backText: 'Add Meesho Account', icon: '+' },
    { id: 'fyp',         label: 'FYP',         value: '—',                          detail: 'Recommended Products',        backText: 'Recommended Products', icon: '◆' },
  ];

  const orderTapTimers = useRef({});
  const [orderTapState, setOrderTapState] = useState({});

  const openOrderDetails = useCallback((order, key) => {
    if (orderTapTimers.current[key]) { window.clearTimeout(orderTapTimers.current[key]); delete orderTapTimers.current[key]; }
    setOrderTapState(current => ({ ...current, [key]: false }));
    setSelectedOrder(order);
  }, []);

  const handleOrderCardTap = useCallback((order, key) => {
    if (orderTapTimers.current[key]) {
      window.clearTimeout(orderTapTimers.current[key]);
      delete orderTapTimers.current[key];
      openOrderDetails(order, key);
      return;
    }
    setOrderTapState(current => ({ ...current, [key]: true }));
    orderTapTimers.current[key] = window.setTimeout(() => { delete orderTapTimers.current[key]; }, 650);
  }, [openOrderDetails]);

  return (
    <main className="app-page">
      <LoadingOverlay onComplete={() => setPageReady(true)} />
      <div className="app-page__ambient" aria-hidden="true" />

      {selectedProduct && (
        <ProductDetailPage
          product={selectedProduct}
          onClose={() => setSelectedProduct(null)}
          onAddToCart={(p, qty, v) => addProductToCart(p, qty, false, v)}
          onBuyNow={(p, qty, v) => addProductToCart(p, qty, true, v)}
        />
      )}

      {showCartAddressOverlay && cartAccountId && (
        <CartAddressAddOverlay
          accountId={cartAccountId}
          onClose={() => setShowCartAddressOverlay(false)}
          onSaved={async () => {
            setShowCartAddressOverlay(false);
            // Reload addresses for cart
            try {
              const r = await fetchAddresses(cartAccountId);
              const addrs = r?.data?.addresses ?? r?.addresses ?? [];
              setCartAddresses(Array.isArray(addrs) ? addrs : []);
              const newest = addrs[addrs.length - 1] ?? addrs[0];
              if (newest) {
                setCartAddressId(newest.id);
                // Auto bind the newly added address
                await handleBindCartAddress(newest.id, newest.pin ?? newest.pincode);
              }
            } catch {}
          }}
        />
      )}

      <header className="app-page__header">
        <div>
          <p className="app-page__eyebrow">MesoWeb</p>
          <h1>{activeItem?.label || actionCards.find(c => c.id === active)?.label || 'Home'}</h1>
        </div>
        <span className="app-page__brand">By SEV7N</span>
      </header>

      <section className="app-page__content" aria-live="polite">

        {/* ── HOME ── */}
        {active === 'home' && (
          <div className="app-page__dashboard">
            <div className="app-page__stats">
              {[
                { title: 'Accounts',  value: statsReady ? String(dashStats.total_accounts) : String(homeStats.total),     percent: '—', tone: 'accounts'  },
                { title: 'Success',   value: statsReady ? String((dashStats.shipped ?? 0) + (dashStats.delivered ?? 0)) : String(homeStats.success),   percent: '—', tone: 'success'   },
                { title: 'Cancelled', value: statsReady ? String(dashStats.cancelled)      : String(homeStats.cancelled), percent: '—', tone: 'cancelled' },
              ].map(card => (
                <article className={`card card--${card.tone}${pageReady ? ' is-ready' : ''}`} key={card.title}>
                  <div className="title">
                    <span aria-hidden="true"><svg width="20" height="20" fill="currentColor" viewBox="0 0 24 24"><rect x="4" y="12" width="3" height="7" rx="1"/><rect x="10.5" y="8" width="3" height="11" rx="1"/><rect x="17" y="4" width="3" height="15" rx="1"/></svg></span>
                    <p className="title-text">{card.title}</p>
                  </div>
                  <div className="data"><p>{card.value}</p><div className="range"><div className="fill" /></div></div>
                </article>
              ))}
            </div>

            <div className="meso-card-grid">
              {actionCards.map((card, i) => (
                <FlipCard key={card.id} card={card} pageReady={pageReady} delay={i * 0.1} onOpen={id => { setActive(id); }} />
              ))}
            </div>

            <section className="home-credits">
              <div className="home-credits__stars container" aria-hidden="true"><div id="stars"/><div id="stars2"/><div id="stars3"/></div>
              <div className="home-credits__sticker"><img className={homeCreditAvatarVisible ? 'is-visible' : ''} src={homeCreditAvatarUrl} alt="Clash Royale emote" /></div>
              <div className="home-credits__text"><strong>Created By SEV7N</strong><span>With Help of Claude &amp; ProxyBin</span></div>
            </section>

            <section className="recent-updates" aria-label="Recent Updates">
              <div className="recent-updates__header">
                <div><p className="app-page__eyebrow">MesoWeb</p><h2>Recent Updates</h2></div>
                <button type="button" className={`recent-updates__refresh${statsRefreshing ? ' is-refreshing' : ''}`} onClick={handleStatsRefresh} disabled={statsRefreshing}>
                  {statsRefreshing ? 'Refreshing…' : '↻ Refresh'}
                </button>
              </div>
              <div className="recent-updates__list">
                {recentUpdates.length === 0 && accounts.length === 0 && (
                  <article><span className="recent-updates__dot"/><div><strong>No accounts yet</strong></div><time>—</time></article>
                )}
                {recentUpdates.map((u, i) => (
                  <article key={i}>
                    <span className="recent-updates__dot"/>
                    <div><strong>+91 {u.phone} — order just got <span className={`recent-updates__status recent-updates__status--${u.next?.toLowerCase()}`}>{u.next}</span></strong></div>
                    <time>{u.time}</time>
                  </article>
                ))}
                {recentUpdates.length === 0 && accounts.length > 0 && (
                  <article><span className="recent-updates__dot"/><div><strong>No status changes detected yet</strong></div><time>—</time></article>
                )}
              </div>
            </section>
          </div>
        )}

        {/* ── ACCOUNTS ── */}
        {active === 'accounts' && (
          <div className="app-page__dashboard accounts-dashboard">
            <div className="app-page__stats">
              {[
                { title: 'Total Accounts', value: statsReady ? String(dashStats.total_accounts) : String(accounts.length),                                                                          tone: 'accounts'  },
                { title: 'FOD Available',  value: statsReady ? String(dashStats.fod_available)  : String(accounts.filter(a => !a.last_order_status || a.last_order_status === '—').length),          tone: 'unused'    },
                { title: 'FOD Used',       value: statsReady ? String(dashStats.fod_used)        : String(accounts.reduce((t, a) => t + (a.orders_placed ?? 0), 0) || accounts.length),              tone: 'cancelled' },
                { title: 'Shipped',        value: statsReady ? String(dashStats.shipped)         : String(accounts.filter(a => a.last_order_status === 'Shipped').length),                           tone: 'shipped'   },
                { title: 'Cancelled',      value: statsReady ? String(dashStats.cancelled)       : String(accounts.filter(a => a.last_order_status === 'Cancelled').length),                         tone: 'cancelled' },
                { title: 'Delivered',      value: statsReady ? String(dashStats.delivered)       : String(accounts.filter(a => a.last_order_status === 'Delivered').length),                         tone: 'delivered' },
              ].map(card => (
                <article className={`card card--${card.tone}${pageReady ? ' is-ready' : ''}`} key={card.title}>
                  <div className="title">
                    <span aria-hidden="true"><svg width="20" height="20" fill="currentColor" viewBox="0 0 24 24"><rect x="4" y="12" width="3" height="7" rx="1"/><rect x="10.5" y="8" width="3" height="11" rx="1"/><rect x="17" y="4" width="3" height="15" rx="1"/></svg></span>
                    <p className="title-text">{card.title}</p>
                  </div>
                  <div className="data"><p>{card.value}</p><div className="range"><div className="fill"/></div></div>
                </article>
              ))}
            </div>

            <section className="accounts-table">
              <div className="accounts-table__header"><span>Phone</span><span>FOD</span><span>Status</span><span>Actions</span></div>
              {accountsLoading && <div className="accounts-table__loading">Loading accounts…</div>}
              <div className="accounts-table__body">
                {accounts.map(acc => (
                  <div className="accounts-table__row" key={acc.account_id}>
                    <span className="accounts-table__id">{acc.phone}</span>
                    <span className="accounts-table__fod">{acc.fod_bucket ? `₹${Number(acc.fod_bucket).toLocaleString('en-IN')}` : '—'}</span>
                    <span className={`accounts-table__status accounts-table__status--${(acc.last_order_status ?? 'pending').toLowerCase().replace(/\s/g,'-')}`}>{acc.last_order_status ?? '—'}</span>
                    <div className="accounts-table__actions">
                      <button type="button" className="cssbuttons-io"
                        onClick={async () => {
                          try { await navigator.clipboard.writeText(JSON.stringify(acc, null, 2)); setCopiedId(acc.account_id); window.setTimeout(() => setCopiedId(null), 1400); } catch {}
                        }}>
                        <span>
                          <svg viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path d="M0 0h24v24H0z" fill="none"/><path d="M24 12l-5.657 5.657-1.414-1.414L21.172 12l-4.243-4.243 1.414-1.414L24 12zM2.828 12l4.243 4.243 1.414-1.414L0 12l5.657-5.657L7.07 7.757 2.828 12zm6.96 9H7.66l6.552-18h2.128L9.788 21z" fill="currentColor"/></svg>
                          {copiedId === acc.account_id ? 'Copied' : 'JSON'}
                        </span>
                      </button>
                      <button type="button" className={`noselect${deleteConfirmId === acc.account_id ? ' is-confirming' : ''}`}
                        onClick={async () => {
                          if (deleteConfirmId === acc.account_id) {
                            await deleteAccount(acc.account_id).catch(() => {});
                            setDeleteConfirmId(null);
                            loadAccounts();
                          } else {
                            setDeleteConfirmId(acc.account_id);
                            window.setTimeout(() => setDeleteConfirmId(c => c === acc.account_id ? null : c), 1800);
                          }
                        }}>
                        <span className="text">{deleteConfirmId === acc.account_id ? 'Confirm' : 'Delete'}</span>
                        <span className="icon"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path d="M24 20.188l-8.315-8.209 8.2-8.282-3.697-3.697-8.212 8.318-8.31-8.203-3.666 3.666 8.321 8.24-8.206 8.313 3.666 3.666 8.237-8.318 8.285 8.203z"/></svg></span>
                      </button>
                    </div>
                  </div>
                ))}
                {!accountsLoading && !accounts.length && (<div className="accounts-table__empty">No accounts yet. Add one to get started.</div>)}
              </div>
            </section>
          </div>
        )}

        {/* ── ADDRESSES ── */}
        {active === 'addresses' && (
          <section className="addresses-page">
            <div className="addresses-page__intro"><p className="app-page__eyebrow">MesoWeb</p><h2>Addresses</h2><p>Manage delivery addresses for your accounts.</p></div>
            <AccountPicker value={addressAccountId} onChange={setAddressAccountId} accounts={accounts} label="Select Account" />

            <section className="addresses-list">
              <div className="addresses-section__header">
                <div><p className="app-page__eyebrow">Saved</p><h3>Addresses on Meesho</h3></div>
                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                  <span>{addressList.length} saved</span>
                  <button type="button" className={`app-page__stats-refresh${addrCacheRefreshing ? ' is-refreshing' : ''}`} onClick={handleAddressRefresh} disabled={addrCacheRefreshing}>
                    {addrCacheRefreshing ? 'Refreshing…' : '↻ Refresh'}
                  </button>
                </div>
              </div>
              {addressLoading && <div className="address-loading">Loading addresses…</div>}
              {!addressLoading && addressAccountId && addressList.length === 0 && <div className="address-empty">No addresses found for this account.</div>}
              <div className="addresses-scroll-container">
                {addressList.map(addr => (
                  <article className="address-card" key={addr.id ?? addr.address_id}>
                    <div className="address-card__top">
                      <div><strong>{addr.name}</strong>{addr.is_default && <span className="address-card__default">Default</span>}</div>
                    </div>
                    <p>{addr.address_line_1}{addr.address_line_2 ? `, ${addr.address_line_2}` : ''}</p>
                    <p>{addr.city}, {addr.state} — {addr.pin ?? addr.pincode}</p>
                    <span className="address-card__phone">{addr.mobile}</span>
                  </article>
                ))}
              </div>
            </section>

            <section className="address-form-card">
              <div className="addresses-section__header"><div><p className="app-page__eyebrow">Delivery</p><h3>Add New Address</h3></div></div>
              <div className="address-form">
                <label><span>Full Name</span><input value={addressForm.name} onChange={e => setAddressForm(f => ({ ...f, name: e.target.value }))} placeholder="Enter full name"/></label>
                <label><span>Mobile Number</span><input inputMode="tel" value={addressForm.mobile} onChange={e => setAddressForm(f => ({ ...f, mobile: e.target.value }))} placeholder="+91 98765 43210"/></label>
                <label>
                  <span>Pincode {pincodeResolving ? '(resolving…)' : ''}</span>
                  <input inputMode="numeric" value={addressForm.pincode}
                    onChange={e => setAddressForm(f => ({ ...f, pincode: e.target.value.replace(/\D/g,'').slice(0,6) }))}
                    onBlur={handlePincodeBlur} placeholder="411045"/>
                </label>
                <div className="address-form__split">
                  <label><span>City</span><input value={addressForm.city} onChange={e => setAddressForm(f => ({ ...f, city: e.target.value }))} placeholder="City"/></label>
                  <label><span>State</span><input value={addressForm.state} onChange={e => setAddressForm(f => ({ ...f, state: e.target.value }))} placeholder="State"/></label>
                </div>
                <label><span>Address Line 1</span><input value={addressForm.line1} onChange={e => setAddressForm(f => ({ ...f, line1: e.target.value }))} placeholder="House / Flat / Building"/></label>
                <label><span>Address Line 2 <em>Optional</em></span><input value={addressForm.line2} onChange={e => setAddressForm(f => ({ ...f, line2: e.target.value }))} placeholder="Area / Street / Landmark"/></label>
                <button type="button" className="address-save-button" disabled={addressSaving}
                  onClick={async () => {
                    if (!addressAccountId || !addressForm.name || !addressForm.mobile || !addressForm.pincode || !addressForm.city || !addressForm.state || !addressForm.line1) return;
                    setAddressSaving(true);
                    try {
                      await createAddress({
                        account_id: addressAccountId,
                        name: addressForm.name, mobile: addressForm.mobile,
                        pincode: addressForm.pincode, city: addressForm.city, state: addressForm.state,
                        address_line_1: addressForm.line1, address_line_2: addressForm.line2,
                        address_type: 'Home',
                      });
                      setAddressSaved(true);
                      clearAddressCache();
                      setAddressForm({ name:'', mobile:'', pincode:'', city:'', state:'', line1:'', line2:'', isDefault: false });
                      fetchAddresses(addressAccountId).then(r => setAddressList(r?.data?.addresses ?? r?.addresses ?? [])).catch(() => {});
                    } catch { alert('Failed to save address.'); }
                    setAddressSaving(false);
                  }}>
                  {addressSaved ? 'Address Saved ✓' : addressSaving ? 'Saving…' : 'Save Address'}
                </button>
              </div>
            </section>
          </section>
        )}

        {/* ── ADD ACCOUNT ── */}
        {active === 'add-account' && (
          <section className="add-account-page">
            <div className="add-account-form-card">
              <div className="add-account-form-header"><span className="add-account-form-icon">+</span><div><p>Add Account</p><span>Add your Meesho account details</span></div></div>
              <form className="add-account-form" onSubmit={e => { e.preventDefault(); if (phoneNumber.length === 10) setAccountSubmitted(true); }}>
                <label className="add-account-field">
                  <span>Phone Number</span>
                  <div className="add-account-phone-wrap">
                    <input type="tel" inputMode="numeric" placeholder="Enter 10 digit phone number" value={phoneNumber} onChange={handlePhoneChange} maxLength={10}/>
                    {phoneNumber.length === 10 && (
                      <div className="add-account-phone-status">
                        <span className={`msho-status ${mshoStatus === 'bad' ? 'msho-status--bad' : 'msho-status--good'}`}>
                          <span className="msho-status__text">MSHO</span>
                          <span className={`msho-status__mark ${mshoStatus === 'bad' ? 'msho-status__mark--tick' : 'msho-status__mark--cross'}`}/>
                        </span>
                      </div>
                    )}
                  </div>
                </label>
                <div className="add-account-field">
                  <span>Referal</span>
                  <div className={`add-account-input-wrap${referralEditAttempt ? ' is-shaking is-error' : ''}`}>
                    <input type="text" value="2560ev" readOnly aria-readonly="true" onClick={handleReferralAttempt} onKeyDown={handleReferralAttempt}/>
                    <button type="button" className="add-account-lock" onClick={handleReferralAttempt}>
                      <svg viewBox="0 0 24 24"><rect x="5" y="10" width="14" height="10" rx="2"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/></svg>
                    </button>
                  </div>
                </div>
                <button className="add-account-submit" type="submit">Add Account</button>
              </form>

              {fodLoading && (
                <div className="fod-loading" role="status"><div className="fod-loader"><FodLoader type={fodLoader % 5} visible={fodLoaderVisible}/></div><strong>Hunting FOD</strong></div>
              )}

              {fodReady && !otpState && !loginSuccess && (
                <section className="fod-results">
                  <div className="fod-result-card">
                    <p>Login with <strong>+91{phoneNumber}</strong></p>
                    <div className="fod-result-value"><span>Max FOD ₹{fodResult?.max_fod_bucket ? fodResult.max_fod_bucket.toLocaleString('en-IN') : '—'}</span><b>✓</b></div>
                  </div>
                  {otpError && <div className="otp-error">{otpError}</div>}
                  <div className="fod-actions">
                    <button type="button" onClick={handleSendOtp} disabled={otpSending}>{otpSending ? 'Sending…' : 'Send OTP'}</button>
                    <button type="button" onClick={() => { setPhoneNumber(''); setAccountSubmitted(false); setFodReady(false); setFodResult(null); }}>Change Number</button>
                    <button type="button" onClick={() => { setAccountSubmitted(false); window.setTimeout(() => setAccountSubmitted(true), 50); }}>Retry FOD</button>
                  </div>
                </section>
              )}

              {otpState && !loginSuccess && (
                <section className="otp-section fod-results">
                  <div className="fod-result-card"><p>OTP sent to <strong>+91{phoneNumber}</strong></p></div>
                  {otpError && <div className="otp-error">{otpError}</div>}
                  <label className="add-account-field"><span>Enter OTP</span>
                    <input type="tel" inputMode="numeric" placeholder="6-digit OTP" value={otpCode} onChange={e => setOtpCode(e.target.value.replace(/\D/g,'').slice(0,6))} maxLength={6}/>
                  </label>
                  <div className="fod-actions">
                    <button type="button" onClick={handleVerifyOtp} disabled={otpVerifying || otpCode.length < 4}>{otpVerifying ? 'Verifying…' : 'Verify OTP'}</button>
                    <button type="button" onClick={handleSendOtp} disabled={otpSending}>{otpSending ? 'Resending…' : 'Resend OTP'}</button>
                  </div>
                </section>
              )}

              {loginSuccess && (
                <section className="fod-results">
                  <div className="fod-result-card"><p><strong>+91{phoneNumber}</strong> added successfully!</p><div className="fod-result-value"><span>Account Created</span><b>✓</b></div></div>
                  <div className="fod-actions">
                    <button type="button" onClick={() => { setPhoneNumber(''); setAccountSubmitted(false); setFodReady(false); setFodResult(null); setOtpState(null); setLoginSuccess(false); setOtpCode(''); }}>Add Another</button>
                    <button type="button" onClick={() => setActive('accounts')}>View Accounts</button>
                  </div>
                </section>
              )}
            </div>
          </section>
        )}

        {/* ── SEARCH ── */}
        {active === 'search' && (
          <div className="search-page">
            <div className="search-page__intro"><p className="app-page__eyebrow">MesoWeb</p><h2>Search</h2><p>Find products or paste a product link.</p></div>
            <AccountPicker value={searchAccountId || 'anonymous'} onChange={value => setSearchAccountId(value === 'anonymous' ? '' : value)} accounts={accounts} label="Account" includeAnonymous />
            <form className="meso-search" onSubmit={handleSearch}>
              <div className="meso-search__shadow"/>
              <input type="text" value={searchQuery} onChange={e => { setSearchQuery(e.target.value); setSearchSubmitted(false); }} placeholder="Paste Link Or Search Products"/>
              <button type="submit">
                <svg fill="none" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20"><path d="M4 9a5 5 0 1110 0A5 5 0 014 9zm5-7a7 7 0 104.2 12.6.999.999 0 00.093.107l3 3a1 1 0 001.414-1.414l-3-3a.999.999 0 00-.107-.093A7 7 0 009 2z" fillRule="evenodd" fill="currentColor"/></svg>
              </button>
            </form>
            {searchLoading && <div className="search-loading">Searching…</div>}
            {searchSubmitted && !searchLoading && (
              <div className="search-results">
                <div className="search-results__header">
                  <span>{searchResults.length} {searchResults.length === 1 ? 'product' : 'products'}</span>
                  <small>{searchQuery ? `Results for "${searchQuery}"` : ''}</small>
                </div>
                <ProductGrid pageReady={pageReady} products={searchResults} onOpenDetail={openProductDetail}/>
              </div>
            )}
          </div>
        )}

        {/* ── FYP ── */}
        {active === 'fyp' && (
          <div className="fyp-page">
            <div className="fyp-page__intro">
              <div className="fyp-page__title-row">
                <div><p className="app-page__eyebrow">MesoWeb</p><h2>For You</h2><p>Recommended products picked for the selected account.</p></div>
                <div className="fyp-account-dropdown">
                  <button type="button" className="fyp-account-dropdown__trigger"
                    onClick={e => { const open = e.currentTarget.getAttribute('aria-expanded') !== 'true'; e.currentTarget.setAttribute('aria-expanded', String(open)); e.currentTarget.classList.toggle('is-open', open); e.currentTarget.nextElementSibling?.classList.toggle('is-open', open); }}
                    aria-expanded="false" aria-haspopup="listbox">
                    <span className="fyp-account-dropdown__current">
                      <span className="fyp-account-dropdown__avatar">
                        {fypAccountId === 'anonymous' ? 'A' : accounts.find(a => a.account_id === fypAccountId)?.phone?.slice(-2) ?? '?'}
                      </span>
                      <span>
                        <small>Account</small>
                        <strong>{fypAccountId === 'anonymous' ? 'Anonymous' : accounts.find(a => a.account_id === fypAccountId)?.phone ?? fypAccountId}</strong>
                      </span>
                    </span>
                    <svg viewBox="0 0 24 24" fill="none"><path d="m7 9 5 5 5-5"/></svg>
                  </button>
                  <div className="fyp-account-dropdown__menu" role="listbox">
                    {[{ account_id: 'anonymous', phone: 'Anonymous (Your IP)' }, ...accounts].map(a => (
                      <button type="button" key={a.account_id}
                        className={`fyp-account-dropdown__option${fypAccountId === a.account_id ? ' is-selected' : ''}`}
                        onClick={e => {
                          setFypAccountId(a.account_id);
                          const dd = e.currentTarget.closest('.fyp-account-dropdown');
                          dd?.querySelector('.fyp-account-dropdown__trigger')?.setAttribute('aria-expanded','false');
                          dd?.querySelector('.fyp-account-dropdown__trigger')?.classList.remove('is-open');
                          dd?.querySelector('.fyp-account-dropdown__menu')?.classList.remove('is-open');
                        }}
                        role="option" aria-selected={fypAccountId === a.account_id}>
                        <span className="fyp-account-dropdown__avatar">{a.account_id === 'anonymous' ? 'A' : a.phone?.slice(-2)}</span>
                        <span><strong>{a.account_id === 'anonymous' ? 'Anonymous' : a.phone}</strong></span>
                        <i>{fypAccountId === a.account_id ? '✓' : ''}</i>
                      </button>
                    ))}
                  </div>
                </div>
              </div>
            </div>
            {fypLoading && <div className="fyp-loading">Loading recommendations…</div>}
            <ProductGrid pageReady={pageReady} products={fypProducts} onOpenDetail={openProductDetail}/>
          </div>
        )}

        {/* ── CART ── */}
        {active === 'cart' && !checkoutStep && !paymentPage && (
          <div className="cart-page">
            <div className="cart-page__intro">
              <p className="app-page__eyebrow">MesoWeb</p>
              <h2>Cart</h2>
              {cartAccountId
                ? <p>Ordering from <strong>{accounts.find(a => a.account_id === cartAccountId)?.phone ?? cartAccountId}</strong></p>
                : <p>Select an account to order</p>}
            </div>

            <CartAccountPicker value={cartAccountId} onChange={setCartAccountId} accounts={accounts} />

            {/* Delivery address strip */}
            {cartAccountId && (
              <section className="cart-page__section cart-address-section">
                <div className="cart-page__section-header">
                  <div><p className="app-page__eyebrow">Deliver to</p><h3>Delivery Address</h3></div>
                  <button type="button" className="cart-page__button cart-page__button--small" onClick={() => setShowCartAddressOverlay(true)}>+ Add</button>
                </div>
                {cartAddressLoading && <div className="cart-address-loading">Loading addresses…</div>}
                {!cartAddressLoading && cartAddresses.length === 0 && (
                  <div className="cart-address-empty">No addresses yet. Tap + Add to create one.</div>
                )}
                <div className="cart-address-strip">
                  {cartAddresses.map(addr => (
                    <button
                      type="button"
                      key={addr.id}
                      className={`cart-address-chip${cartAddressId === addr.id ? ' is-selected' : ''}`}
                      onClick={() => handleBindCartAddress(addr.id, addr.pin ?? addr.pincode)}
                    >
                      <strong>{addr.name}</strong>
                      <span>{addr.city} — {addr.pin ?? addr.pincode}</span>
                    </button>
                  ))}
                </div>
              </section>
            )}

            <form className="meso-search" onSubmit={e => e.preventDefault()}>
              <div className="meso-search__shadow"/>
              <input type="text" placeholder="Paste Meesho Product Link" value={productLink} onChange={e => setProductLink(e.target.value)}/>
            </form>

            <div className="cart-page__actions">
              <button className="cart-page__button" type="button" onClick={handleFetchProduct} disabled={fetchingProduct}>
                {fetchingProduct ? 'Fetching…' : 'Fetch Product'}
              </button>
              <button className="cart-page__button" type="button" onClick={handleFetchMyCart}>Fetch My Cart</button>
            </div>

            {fetchedProduct && (
              <section className={`cart-page__section cart-fetched-product ${productMovingToCart ? 'is-moving-to-cart' : ''}${productDisappearing ? ' is-disappearing' : ''}`}>
                <div className="cart-page__section-header">
                  <div><p className="app-page__eyebrow">Fetched product</p><h3>{fetchedProduct.name}</h3></div>
                  <span>{fetchedProduct.price}</span>
                </div>
                <div className="cart-fetched-product__body">
                  <img src={fetchedProduct.image || null} alt=""/>
                  <div className="cart-fetched-product__info">
                    <strong>{fetchedProduct.name}</strong>
                    <span>{fetchedProduct.category}</span>
                    <div className="cart-fetched-product__pricing">
                      {fetchedProduct.mrpPrice && (<del className="product-detail__mrp">{fetchedProduct.mrpPrice}</del>)}
                      <b className="product-detail__cod">{fetchedProduct.price}</b>
                      {fetchedProduct.upiPrice && (
                        <span className="product-detail__upi-glow"><svg className="product-detail__upi-icon" viewBox="0 0 24 24" aria-hidden="true"><path d="M13.2 2 5 13h6l-.8 9L19 10h-6z"/></svg>{fetchedProduct.upiPrice} UPI PRICE</span>
                      )}
                    </div>
                  </div>
                </div>
                <div className="cart-fetched-product__actions">
                  <button className={`cart-add-button ${productAdded ? 'is-added' : ''}`} type="button" onClick={addFetchedToCart}>
                    <span className="cart-add-button__circle">
                      <span className="cart-add-button__cart"><svg viewBox="0 0 24 24"><path d="M3 4h2l2.2 10.2a2 2 0 0 0 2 1.6h7.7a2 2 0 0 0 1.9-1.4L21 8H7.1M10 20h.01M18 20h.01"/></svg></span>
                      <span className="cart-add-button__wind"/>
                    </span>
                    <span className="cart-add-button__text">{productAdded ? 'Added' : 'Add to Cart'}</span>
                  </button>
                  <button className="cart-fetched-product__cancel" type="button" onClick={() => setFetchedProduct(null)}>Cancel</button>
                </div>
              </section>
            )}

            <section className="cart-page__section">
              <div className="cart-page__section-header">
                <div><p className="app-page__eyebrow">Your cart</p><h3>Items in Cart</h3></div>
                <span>{cartItems.length} {cartItems.length === 1 ? 'item' : 'items'}</span>
              </div>
              <div className="cart-items-list">
                {cartItems.map(item => (
                  <div className={`cart-items-list__row ${cartJustAddedId === item.id ? 'is-new' : ''}`} key={item.id}>
                    <img src={item.image} alt="" />
                    <div className="cart-items-list__info">
                      <strong>{item.name}</strong>
                      <span>{item.quantity} {item.quantity === 1 ? 'piece' : 'pieces'}</span>
                    </div>
                    <div className="cart-items-list__amount">
                      <span>Amount</span>
                      <strong>{item.price}</strong>
                      <div className="cart-quantity">
                        <button type="button" onClick={() => changeCartQuantity(item, -1)}>−</button>
                        <b>{item.quantity}</b>
                        <button type="button" onClick={() => changeCartQuantity(item, +1)}>+</button>
                      </div>
                      <button className="cart-item-delete" type="button" onClick={() => removeCartItem(item)}>Delete</button>
                    </div>
                  </div>
                ))}
                {cartItems.length === 0 && <div className="cart-empty">Cart is empty. Fetch a product or paste a link.</div>}
              </div>
            </section>

            <button className="cart-page__button cart-page__button--checkout" type="button" onClick={() => setCheckoutStep(true)} disabled={!cartItems.length || !cartAddressId}>
              Proceed to Checkout
            </button>
          </div>
        )}

        {/* ── CHECKOUT ── */}
        {active === 'cart' && checkoutStep && (
          <div className="checkout-page">
            <div className="checkout-page__intro">
              <p className="app-page__eyebrow">MesoWeb</p>
              <h2>Payment Method</h2>
              {cartAccountId && <p>Ordering from <strong>{accounts.find(a => a.account_id === cartAccountId)?.phone}</strong></p>}
            </div>

            {/* Read-only delivery address */}
            <section className="checkout-address checkout-address--readonly">
              <div className="checkout-page__summary-header">
                <div><p className="app-page__eyebrow">Delivery</p><h3>Delivering To</h3></div>
              </div>
              {selectedAddress ? (
                <div className="checkout-address__preview">
                  <div><span>Recipient</span><strong>{selectedAddress.name}</strong></div>
                  <p>{selectedAddress.address_line_1}{selectedAddress.address_line_2 ? `, ${selectedAddress.address_line_2}` : ''}, {selectedAddress.city}, {selectedAddress.state} — {selectedAddress.pin ?? selectedAddress.pincode}</p>
                  <span>{selectedAddress.mobile}</span>
                </div>
              ) : (
                <div className="checkout-no-addr">No address selected. Go back and pick one.</div>
              )}
            </section>

            <section className="checkout-page__summary">
              <div className="checkout-page__summary-header">
                <div><p className="app-page__eyebrow">Your order</p><h3>Order Summary</h3></div>
                <span>{cartItems.length} {cartItems.length === 1 ? 'item' : 'items'}</span>
              </div>
              <div className="checkout-page__summary-items">
                {cartItems.map(item => (
                  <div className="checkout-order-item" key={item.id}>
                    <img src={item.image} alt=""/>
                    <div className="checkout-order-item__info">
                      <strong>{item.name}</strong>
                      <span>{item.category}</span>
                      <small>{item.quantity} {item.quantity === 1 ? 'piece' : 'pieces'}</small>
                    </div>
                    <strong className="checkout-order-item__price">₹{(Number((item.price||'').replace(/[^0-9]/g,'')) * item.quantity).toLocaleString('en-IN')}</strong>
                  </div>
                ))}
              </div>
            </section>

            <section className="cart-fod-highlight">
              <div className="cart-fod-highlight__gift"><span className="gift-box"><i/><b/></span></div>
              <div><span>FOD Applied</span><strong>- ₹{fodDiscount}</strong></div>
              <b>Offer applied</b>
            </section>

            <section className="cart-page__section checkout-page__bill">
              <div className="cart-page__section-header"><div><p className="app-page__eyebrow">Final calculation</p><h3>Bill Details</h3></div></div>
              <div className="cart-bill">
                <div><span>Product Price</span><strong>₹{cartSubtotal.toLocaleString('en-IN')}</strong></div>
                <div><span>FOD on Product</span><strong className="cart-bill__discount">- ₹{fodDiscount}</strong></div>
                <div><span>Cash On Delivery</span><strong>₹{codTotal.toLocaleString('en-IN')}</strong></div>
                <div><span>Via UPI</span><strong>₹{upiTotal.toLocaleString('en-IN')}</strong></div>
                <div className="cart-bill__total"><span>You Pay</span><strong>₹{(paymentMethod === 'UPI' ? upiTotal : codTotal).toLocaleString('en-IN')}</strong></div>
              </div>
            </section>

            <section className="payment-methods">
              <button type="button" className={`payment-method ${paymentMethod === 'COD' ? 'is-selected' : ''}`} onClick={() => setPaymentMethod('COD')}>
                <span className="payment-method__icon">₹</span>
                <span><strong>Cash on Delivery</strong><small>Pay when your order arrives</small></span>
                <b>₹{codTotal.toLocaleString('en-IN')}</b>
              </button>
              <button type="button" className={`payment-method ${paymentMethod === 'UPI' ? 'is-selected' : ''}`} onClick={() => setPaymentMethod('UPI')}>
                <span className="payment-method__icon">UPI</span>
                <span><strong>UPI</strong><small>Pay securely online</small></span>
                <b>₹{upiTotal.toLocaleString('en-IN')}</b>
              </button>
            </section>

            <div className="checkout-page__total">
              <span>You Pay</span>
              <strong>₹{(paymentMethod === 'UPI' ? upiTotal : codTotal).toLocaleString('en-IN')}</strong>
            </div>

            <button className="cart-page__button checkout-page__place" type="button" onClick={startPaymentPage}>Place Order</button>
            <button className="checkout-page__back" type="button" onClick={() => setCheckoutStep(false)}>Back to Cart</button>
          </div>
        )}

        {/* ── PAYMENT ── */}
        {paymentPage && paymentOrder && (
          <div className="payment-page" key={paymentOrder.id}>
            <PaymentPrintAnimation paymentOrder={paymentOrder}/>
            {paymentOrder.paymentMethod === 'COD' ? (
              <div className="payment-page__cod-actions">
                <button className="cart-page__button payment-page__complete" type="button" onClick={completeOrder}>Confirm Order</button>
                <button className="payment-page__cancel" type="button" onClick={() => { setPaymentPage(false); setPaymentOrder(null); setCheckoutStep(true); }}>Cancel</button>
              </div>
            ) : (
              <div className="payment-page__upi-actions">
                <label className={`payment-light-button payment-light-button--${paymentOrder.paymentLightColor || 'green'}${paymentChecking ? ' is-on' : ''}`} htmlFor={`payment-light-${paymentOrder.id}`}>
                  <input id={`payment-light-${paymentOrder.id}`} name={`payment-light-${paymentOrder.id}`} type="checkbox"
                    checked={paymentChecking}
                    onChange={e => { const c = e.target.checked; setPaymentChecking(c); if (c) window.setTimeout(completeOrder, 350); }}/>
                  <span className="payment-light-button__socket"/>
                  <span className="payment-light-button__bulb">
                    <svg fill="none" viewBox="0 0 131 151" width="22"><path strokeWidth="8" stroke="currentColor" d="M1.00043 50.4999C80.0004 57.4999 102 50.4999 102 50.4999C102 50.4999 125 45.9999 127 31.4999C129 16.9998 115 1.49988 107.5 3.49988C100 5.49988 83.5004 16.9999 83.5004 75.4999C83.5004 83.9786 83.8466 91.4701 84.4622 98.0884M1 100.5C43.5028 96.7338 69.5067 97.0201 84.4622 98.0884M84.4622 98.0884C97.3045 99.0058 102 100.5 102 100.5C102 100.5 125 105 127 119.5C129 134 115 149.5 107.5 147.5C101.087 145.79 88.0938 137.134 84.4622 98.0884Z"/></svg>
                    <span className="payment-light-button__text">Checking payment..</span>
                  </span>
                </label>
                <button className="payment-page__cancel" type="button" onClick={() => { setPaymentChecking(false); setPaymentPage(false); setPaymentOrder(null); setCheckoutStep(true); }}>Cancel</button>
              </div>
            )}
          </div>
        )}

        {/* ── ORDERS ── */}
        {active === 'orders' && (
          <section className="orders-page">
            <div className="orders-page__intro"><p className="app-page__eyebrow">MesoWeb</p><h2>Recent Orders</h2><p>Latest orders from your accounts</p></div>
            <div className="orders-account-selector"><AccountPicker value={ordersAccountId} onChange={setOrdersAccountId} accounts={accounts} label="Account" includeAll /></div>

            {ordersLoading && (
              <div className="orders-loading">
                <FodLoader type={3} visible />
                <span>Loading orders…</span>
                <button type="button" onClick={() => {
                  setOrdersLoading(true); setOrdersRetrying(true);
                  const refresh = async () => {
                    try {
                      const results = await Promise.all(accounts.map(async (acc) => {
                        const r = await fetchOrders(acc.account_id);
                        return { acc, orders: (r.orders ?? []).map(o => ({ ...o, phone: acc.phone, accountId: acc.account_id })) };
                      }));
                      let nextCache = readOrdersCache();
                      results.forEach(({ acc, orders }) => { nextCache = mergeCachedOrders(nextCache, acc.account_id, acc.phone, orders); });
                      writeOrdersCache(nextCache);
                      const nextOrders = getCachedOrdersForAccounts(nextCache, accounts, ordersAccountId);
                      setOrdersList(nextOrders);
                      setOrdersLoading(nextOrders.length === 0);
                    } catch {} finally { setOrdersRetrying(false); }
                  };
                  void refresh();
                }} disabled={ordersRetrying}>{ordersRetrying ? 'Retrying…' : 'Retry now'}</button>
              </div>
            )}

            <div className="orders-list">
              {ordersList.map((order, i) => {
                const statusText = typeof order.status === 'string' ? order.status : order.status?.title?.text ?? '—';
                const dateStr = order.date ?? (order.created_date ? new Date(order.created_date).toLocaleDateString('en-IN') : '—');
                const rawPaymentMode = order.payment_mode ?? order.payment_details?.final_payment_mode ?? '';
                const paymentValue = String(rawPaymentMode).toLowerCase().replace(/[^a-z0-9]/g, '');
                const isCod = /cod|cash.?on.?delivery/i.test(paymentValue);
                const isWallet = /meesho.?balance|wallet/i.test(paymentValue);
                const paymentLabel = isCod ? 'COD' : isWallet ? 'Wallet' : 'UPI';
                const paymentTone = isCod ? 'cod' : isWallet ? 'wallet' : 'upi';
                const orderName = order.product_name ?? order.productName ?? order.product_title ?? order.item_name ?? order.name ?? order.title ?? 'Order';
                const statusClass = String(statusText).trim().toLowerCase().replace(/\s+/g, '-').replace(/[^a-z0-9-]/g, '');

                return (
                  <button type="button" className={`orders-card${orderTapState[order.sub_order_num ?? order.id ?? i] ? ' is-pressed' : ''}`}
                    key={order.sub_order_num ?? order.id ?? i}
                    onClick={() => handleOrderCardTap(order, order.sub_order_num ?? order.id ?? i)}
                    onDoubleClick={(e) => { e.preventDefault(); openOrderDetails(order, order.sub_order_num ?? order.id ?? i); }}>
                    <div className="orders-card__image-wrap">
                      {order.product_image ? (<img className="orders-card__image" src={order.product_image} alt="" />) : (<div className="orders-card__image-fallback" aria-hidden="true">MESO</div>)}
                    </div>
                    <div className="orders-card__info">
                      <div className="orders-card__name" title={orderName}>{orderName}</div>
                      <div className="orders-card__line"><span className="orders-card__label">STATUS</span><strong className={`orders-card__status orders-card__status--${statusClass}`}>{statusText}</strong></div>
                      <div className="orders-card__line"><span className="orders-card__label">PAYMENT</span><strong className={`orders-card__payment orders-card__payment--${paymentTone}`}>{paymentLabel}</strong></div>
                      <div className="orders-card__footer">
                        <span><small>ID</small><strong>{order.sub_order_num ?? order.id ?? '—'}</strong></span>
                        <span><small>Date</small><strong>{dateStr}</strong></span>
                      </div>
                    </div>
                  </button>
                );
              })}
              {!ordersLoading && !ordersList.length && (<div className="orders-empty">Unable to load orders. Retrying automatically…</div>)}
            </div>
          </section>
        )}

        {selectedOrder && (
          <OrderDetailsPage
            order={selectedOrder}
            accountId={selectedOrder.accountId ?? (ordersAccountId !== 'all' ? ordersAccountId : null)}
            onClose={() => setSelectedOrder(null)}
          />
        )}

        {/* ── PROFILE ── */}
        {active === 'profile' && (
          <section className="profile-page">
            <div className="profile-hero">
              <div className="profile-avatar"><img className={profileAvatarVisible ? 'is-visible' : ''} src={profileAvatarUrl} alt="Clash Royale emote"/></div>
              <div className="profile-hero__info"><p className="app-page__eyebrow">MesoWeb</p><h2>Meso User</h2><span>Profile & account overview</span></div>
            </div>

            <section className="profile-details-card">
              <div className="profile-details-card__header"><div><p className="app-page__eyebrow">Account</p><h3>Personal Details</h3></div></div>
              <div className="profile-detail-row"><span>Name</span><strong>Meso User</strong></div>
              <div className="profile-detail-row"><span>Primary Phone</span><strong>{accounts[0]?.phone ?? '—'}</strong></div>
            </section>

            <section className="profile-phone-card">
              <div className="profile-details-card__header"><div><p className="app-page__eyebrow">Accounts</p><h3>Phone Numbers</h3></div><span>{accounts.length} numbers</span></div>
              <div className="profile-phone-list">
                {accounts.map((a, i) => (<div className="profile-phone-row" key={a.account_id}><span>{String(i + 1).padStart(2,'0')}</span><strong>{a.phone}</strong></div>))}
                {!accounts.length && <div className="profile-phone-row"><span>—</span><strong>No accounts</strong></div>}
              </div>
            </section>

            <div className="profile-stats">
              {[
                { title: 'Total Numbers', value: String(accounts.length), tone: 'total' },
                { title: 'Unused', value: String(accounts.filter(a => !a.last_order_status || a.last_order_status === '—').length), tone: 'unused' },
                { title: 'Cancelled', value: String(accounts.filter(a => a.last_order_status === 'Cancelled').length), tone: 'cancelled' },
              ].map(stat => (
                <article className={`profile-stat profile-stat--${stat.tone}`} key={stat.title}>
                  <span>{stat.title}</span><strong>{stat.value}</strong>
                </article>
              ))}
            </div>

            <section className="profile-referral">
              <div><p className="app-page__eyebrow">Referral</p><h3>Referral Code</h3></div>
              <div className={`profile-referral__input-wrap${referralEditAttempt ? ' is-shaking is-error' : ''}`}>
                <input type="text" value="2560ev" readOnly aria-readonly="true" onClick={handleReferralAttempt} onKeyDown={handleReferralAttempt}/>
                <button type="button" className="profile-referral__lock" onClick={handleReferralAttempt}>
                  <svg viewBox="0 0 24 24"><rect x="5" y="10" width="14" height="10" rx="2"/><path d="M8 10V7a4 4 0 0 1 8 0v3"/></svg>
                </button>
              </div>
            </section>
          </section>
        )}

        {/* ── FALLBACK ── */}
        {active !== 'home' && active !== 'search' && active !== 'fyp' && active !== 'add-account' && active !== 'accounts' && active !== 'addresses' && active !== 'cart' && active !== 'orders' && active !== 'profile' && (
          <div className="app-page__detail">
            <p className="app-page__eyebrow">MesoWeb</p>
            <h2>{actionCards.find(c => c.id === active)?.label}</h2>
            <div className="app-page__detail-value">{actionCards.find(c => c.id === active)?.value}</div>
            <p>{actionCards.find(c => c.id === active)?.detail}</p>
            <button type="button" onClick={() => setActive('home')}>Back to Home</button>
          </div>
        )}

      </section>

      <nav className="app-nav glass-radio-group">
        {navItems.map(item => (
          <React.Fragment key={item.id}>
            <input type="radio" name="mesoweb-nav" id={'glass-' + item.id}
              checked={active === item.id}
              onChange={() => { setPaymentPage(false); setPaymentOrder(null); setActive(item.id); }}/>
            <label htmlFor={'glass-' + item.id}>{item.label}</label>
          </React.Fragment>
        ))}
        <div className="glass-glider"/>
      </nav>

      {orderPlacedOverlay && (
        <div className="order-placed-overlay" role="status" aria-live="polite">
          <div className="order-placed-overlay__confetti">{Array.from({ length: 24 }, (_, i) => <i key={i}/>)}</div>
          <div className="order-placed-overlay__card">
            <div className="order-placed-overlay__check">✓</div>
            <p className="app-page__eyebrow">MesoWeb</p>
            <h2>Order Placed</h2>
            <span>{paymentOrder?.id}</span>
          </div>
        </div>
      )}
    </main>
  );
}